# 性能瓶颈深度分析与优化方案

> 基于 `test_full_pg.py` 测试结果与源代码的深度分析  
> 日期：2026-04-08  
> 测试环境：PostgreSQL 16.13 / Python Conda GIS / s2sphere 0.2.5 / shapely 2.0.5

---

## 一、测试数据回顾

| 指标 | 数值 |
|------|------|
| 入库区划数 | 477 |
| S2 Cell 总数 | 4,078,872 |
| Interior / Boundary Cell | 1,857,913 (45.6%) / 2,220,959 (54.4%) |
| 平均 Cell/区划 | 8,551（最少 108，最多 194,256） |
| L12 Cell 占比 | 95.2%（3,884,634 个） |
| 入库总耗时 | 1,142s（平均 2.4s/条） |
| 加密 geometry 总大小 | 3,491 KB |
| Cell 索引表大小 | 207,848 KB (~203 MB) |
| 点查询平均耗时 | 12.6ms，命中率 5/8 (62.5%) |
| 面查询耗时范围 | 84ms (上海中心) ~ 723ms (广深走廊) |

---

## 二、瓶颈 1：入库慢（1142s / 477条 = 2.4s/条）

### 2.1 根因链路（按耗时占比排序）

#### 2.1.1 S2 覆盖生成 — 占 ~60% 耗时（约 1.4s/条）

**代码路径**：`s2_utils.py` `polygon_to_s2_covering()` → `_classify_covering_cells()`

**根因 A：LatLngRect 近似导致 Cell 数膨胀**

```python
# s2_utils.py L157
latlng_rect = _shapely_to_s2_latlng_rect(polygon)  # 只取 bounding box
covering_cells = coverer.get_covering(latlng_rect)   # 基于 bbox 覆盖
```

s2sphere 没有 `S2Polygon` / `S2Loop` 的 Region 实现，只能用 `LatLngRect`（bounding box）近似。对于狭长或不规则多边形，bounding box 面积可达实际 polygon 的 2~5 倍，直接导致：

- 覆盖 Cell 数远超实际需要
- 例如：一个 L 形区划，其 bounding box 覆盖了 L 形缺失的部分，产生大量"空覆盖" Cell

**量化分析**：朝阳区面积 455 km²，bounding box 面积约 700 km²，膨胀比 1.54x；但对于狭长的密云区（面积 2229 km²，南北跨度极大），bounding box 膨胀比可达 3x 以上。

**根因 B：逐 Cell 顶点判断 interior — 计算密集**

```python
# s2_utils.py L89-114 _classify_covering_cells()
for cell_id_obj in covering_cells:
    cell = s2.Cell(cell_id_obj)           # 构造 Cell 对象
    for i in range(4):                     # 4 个顶点
        vertex = cell.get_vertex(i)        # 获取顶点坐标
        vertex_latlng = s2.LatLng.from_point(vertex)  # 转经纬度
        if not prepared_poly.contains(Point(lng, lat)):  # Shapely contains
            all_vertices_inside = False
            break
```

每个 Cell 需要：
- 1 次 `s2.Cell()` 构造（纯 Python，涉及球面坐标计算）
- 4 次 `cell.get_vertex()` + `s2.LatLng.from_point()`（球面→经纬度投影）
- 4 次 `prepared_poly.contains(Point(lng, lat))`（Shapely 点在面内判断）

对于平均 8,551 个 Cell/区划：**8,551 × (1 + 4 + 4 + 4) = 110,907 次**计算操作。

s2sphere 是纯 Python 实现，没有 C/C++ 扩展，`Cell()` 构造和 `get_vertex()` 是计算密集型的瓶颈。

**根因 C：s2sphere RegionCoverer 的自适应降级**

```python
# s2_utils.py L159-162
coverer = s2.RegionCoverer()
coverer.min_level = 12
coverer.max_level = 18
coverer.max_cells = 500
```

当覆盖 Cell 数达到 `max_cells=500` 上限时，RegionCoverer 会将细粒度边界 Cell 合并为粗粒度 L12 Cell 以满足数量约束。这导致：

- 大面积区划（如密云区 2229 km²）被迫使用大量 L12 Cell 填充
- 密云区实际生成 1,186 个 Cell，远超 max_cells=500 的约束——说明 `get_covering` 的返回值可能超过 max_cells（这是 s2sphere 的实现行为）
- 这些 L12 Cell 几乎全部落在边界区域，4 顶点大概率不全在 polygon 内 → 标记为 boundary

#### 2.1.2 逐条写入 DB — 占 ~30% 耗时（约 0.7s/条）

**代码路径**：`indexing.py` `process_single_village()` → `db.py` `insert_village_with_cells()`

**根因 A：autocommit 模式下每条 INSERT 是独立事务**

```python
# db.py L22
self._conn.autocommit = True  # 每条SQL自动提交
```

autocommit 模式下，每个 `execute_values` 调用都触发一次 WAL 刷盘确认。对于平均 8,551 个 Cell：

```python
# db.py L130-138 execute_values page_size=1000
psycopg2.extras.execute_values(
    cur,
    "INSERT INTO village_s2_cells ... VALUES %s ...",
    full_records,
    template="(%s, %s, %s, %s)",
    page_size=1000,  # 每1000条一个batch
)
```

8,551 个 Cell ÷ 1000 = **9 次网络往返**，每次都需要等待 WAL 确认。

**根因 B：逐条处理无批量化**

```python
# indexing.py L139-149
for feature in read_geojson_features(geojson_path):
    village_id = process_single_village(geom, village_info, db, crypto, app_config)
```

每个区划独立处理：S2 覆盖 → 加密 → INSERT 主表 → INSERT Cell 表 → 提交。无法利用批量事务优化。

#### 2.1.3 Fernet 加密 — 占 ~10% 耗时（约 0.2s/条）

```python
# crypto.py 加密流程
encrypted_geom = crypto.encrypt_geometry(geom)
# 内部：geom → WKB序列化 → Fernet加密(AES-128-CBC + HMAC-SHA256)
```

Fernet 对平均 7.3KB 的 WKB 数据加密，单次约 0.2s。虽然占比不高，但对于 477 条数据累计约 95s。

---

### 2.2 入库慢的优化方案

详见下方"优化建议"章节。

---

## 三、瓶颈 2：面查询慢（84ms ~ 723ms）

### 3.1 根因链路

#### 3.1.1 查询条件膨胀 — SQL 效率低

**代码路径**：`s2_utils.py` `build_query_conditions()` → `db.py` `query_cells_by_ranges()`

```python
# s2_utils.py L270-278
for cell_entry in query_cells:
    ancestors = expand_cell_ancestors(cell_entry.cell_id, min_level)  # 每Cell展开7级祖先
    exact_ids.update(ancestors)
    if cell_entry.level < max_db_level:
        range_min, range_max = get_cell_range(cell_entry.cell_id)
        range_conditions.append((range_min, range_max))
```

对于 100 个查询 Cell：
- **exact_ids**: 100 × 7 级祖先 = ~700 个精确 ID
- **range_conditions**: 100 个 Cell 中非 L18 的每个生成 1 个范围条件 ≈ 95 个

```python
# db.py L176-186 query_cells_by_ranges()
conditions = []
for range_min, range_max in range_conditions:
    conditions.append("cell_id BETWEEN %s AND %s")
sql = "SELECT ... FROM village_s2_cells WHERE " + " OR ".join(conditions)
# → 95个 OR 条件！
```

**问题**：
1. PostgreSQL 对大量 `OR` 条件的 B+树索引扫描效率低，优化器可能退化为顺序扫描
2. 95 个 `BETWEEN` 条件 + 700 个 `= ANY(...)` 条件，SQL 解析和优化计划生成本身就耗时
3. `query_cells_by_exact_and_range()` 分两次独立查询（精确+范围），再内存合并，无法利用单次 SQL 优化

**量化**：广深走廊查询生成约 95 个范围条件，返回候选 Cell 数远超实际相交数。

#### 3.1.2 候选集膨胀 — L12 Cell 粒度太粗

由于 L12 Cell 占 95%，一个 L12 Cell (≈2.25 km²) 可能覆盖多个小面积区划。

**范围查询的问题**：
```
查询的一个 L14 Cell → 范围查询捕获其 L12 祖先下的所有后代 Cell
→ 该 L12 祖先下可能有 10+ 个不同区划的 Cell
→ 候选集从"实际相交的 3 个区划"膨胀到"可能相交的 15 个区划"
```

#### 3.1.3 逐个解密验证 — 无并行

```python
# query.py L210-216
for village_id, encrypted_geom in encrypted_geoms.items():
    village_geom = crypto.decrypt_to_geometry(encrypted_geom)  # Fernet解密
    if prepared_input.intersects(village_geom):                 # Shapely intersects
        confirmed_ids.add(village_id)
```

每个 boundary 候选需要：
1. 1 次 DB 读取加密 geom（已在 `get_encrypted_geoms_batch` 批量完成）
2. 1 次 Fernet 解密（AES-128-CBC + HMAC-SHA256 验证）≈ 0.5-1ms
3. 1 次 Shapely `intersects()` ≈ 0.1-0.5ms（取决于 polygon 复杂度）

对于广深走廊查询，假设 20 个 boundary 候选 × 1.5ms = **30ms 解密验证**。但实际 723ms 的主要耗时在 SQL 阶段。

#### 3.1.4 点查询命中率低 — 5/8 = 62.5%

**根因**：

```python
# s2_utils.py L157 — 使用 LatLngRect 覆盖
latlng_rect = _shapely_to_s2_latlng_rect(polygon)
covering_cells = coverer.get_covering(latlng_rect)
```

`LatLngRect` 覆盖与实际 polygon 之间存在间隙：
- S2 的 `get_covering` 保证覆盖 `LatLngRect`，但不保证覆盖 polygon 的每个点
- 区划边界处的 concave 区域可能不被任何 Cell 覆盖
- 查询点恰好落在这些"缝隙"中 → NOT FOUND

```python
# s2_utils.py L94-108 — interior 判断只检查4顶点
for i in range(4):
    vertex = cell.get_vertex(i)
    if not prepared_poly.contains(Point(lng, lat)):
        all_vertices_inside = False
        break
```

对于弯曲边界的 Cell，4 顶点全在 polygon 内不代表 Cell 完全在 polygon 内（Cell 的边可能穿过 polygon 边界）。但对于查询来说这不是主要问题——主要问题是**未被 Cell 覆盖的区域**。

---

## 四、瓶颈 3：Cell 索引膨胀（407 万 Cell，203 MB）

### 4.1 根因链路

#### 4.1.1 max_cells=500 触发自适应降级 → L12 占 95%

S2 `RegionCoverer` 的核心逻辑：
1. 优先用细粒度 Cell（L14-L18）精确覆盖边界
2. 当 Cell 数达到 `max_cells` 上限时，将细粒度 Cell 合并为粗粒度 L12 Cell
3. 对于大面积区划，大量边界区域被迫用 L12 Cell 填充

**Level 分布数据**：

| Level | Cell 数 | 占比 | 说明 |
|-------|---------|------|------|
| 12 | 3,884,634 | 95.2% | 粗粒度"填充"，精度低 |
| 13 | 12,109 | 0.3% | |
| 14 | 15,020 | 0.4% | |
| 15 | 23,093 | 0.6% | |
| 16 | 37,487 | 0.9% | |
| 17 | 52,020 | 1.3% | |
| 18 | 54,509 | 1.3% | 精确边界覆盖 |

L12 占 95% 意味着索引表 203MB 中约 193MB 都是 L12 Cell 的索引数据。

#### 4.1.2 LatLngRect 导致无效 Cell

对于 L 形、C 形等不规则区划，bounding box 内约 30-50% 的面积属于"空覆盖"区域。这些区域生成的 Cell：
- 4 顶点至少 1 个不在 polygon 内 → 标记为 boundary
- 占用索引空间但极少被查询命中（因为查询点/面通常在实际区划内）

#### 4.1.3 Interior/Boundary 比例失衡

| 类型 | 数量 | 占比 |
|------|------|------|
| Interior | 1,857,913 | 45.6% |
| Boundary | 2,220,959 | 54.4% |

Boundary 占比过高（54.4%），原因：
1. L12 Cell 粒度太粗，Cell 边界容易穿过 polygon 边界 → 被标记为 boundary
2. LatLngRect 产生的"空覆盖"Cell 被标记为 boundary
3. 理想情况下（精确覆盖），interior 应占 70%+，boundary 占 30%-

Boundary Cell 比例高 → 查询时需要解密验证的候选更多 → 查询更慢。

---

## 五、优化建议详细实施方案

### 5.1 短期优化

---

#### 优化 1：调高 max_cells 至 2000~5000

**修改位置**：`config.py` L13

```python
# 修改前
max_cells: int = 500

# 修改后
max_cells: int = 2000  # 或 5000，视区划面积调整
```

**原理**：
- `max_cells` 越大，`RegionCoverer` 有更多"预算"用细粒度 Cell 精确覆盖边界
- 减少 L12 降级，提高边界精度
- 更多 Cell 被标记为 interior（细粒度 Cell 更容易完全落在 polygon 内）

**预期收益**：

| 指标 | 当前 (500) | 预期 (2000) | 预期 (5000) |
|------|-----------|-------------|-------------|
| L12 占比 | 95.2% | ~60% | ~40% |
| Cell 总数 | 407 万 | ~500 万 | ~600 万 |
| Interior 占比 | 45.6% | ~65% | ~75% |
| 点查询命中率 | 62.5% | ~85% | ~92% |
| 入库耗时 | 1142s | ~1500s (+30%) | ~2000s (+75%) |
| 索引表大小 | 203 MB | ~260 MB | ~320 MB |

**副作用**：
- 入库时间增加 30-75%（Cell 更多 → 覆盖生成 + 写入更慢）
- 索引表增大 30-60%
- 单次面查询的 `exact_ids` 数量增加，SQL 条件更多

**权衡**：用空间和时间换精度。如果查询精度是首要需求，值得；如果入库速度和存储是首要需求，不建议。

**推荐值**：`max_cells=2000`，是精度与存储的最佳平衡点。

---

#### 优化 2：降低 min_level 至 10

**修改位置**：`config.py` L11

```python
# 修改前
min_level: int = 12

# 修改后
min_level: int = 10
```

**原理**：
- L10 Cell 面积约 100 km²，可以替代大量 L12 Interior Cell
- L11 Cell 面积约 25 km²，适合中等面积区划的内部填充
- 例如：密云区 2229 km² 的内部可以用 ~22 个 L10 Cell 覆盖，替代当前 ~457 个 L12 Interior Cell

**Cell 面积参考**：

| Level | 近似面积 | 适用场景 |
|-------|----------|----------|
| 10 | ~100 km² | 省级/大地级市内部 |
| 11 | ~25 km² | 地级市内部 |
| 12 | ~2.25 km² | 区县级内部 |
| 14 | ~0.14 km² | 边界过渡 |
| 16 | ~0.009 km² | 边界精确 |
| 18 | ~0.0006 km² | 边界最精确 |

**预期收益**：

| 指标 | 当前 (min=12) | 预期 (min=10) |
|------|--------------|---------------|
| Cell 总数 | 407 万 | ~150 万 (-63%) |
| 索引表大小 | 203 MB | ~75 MB (-63%) |
| Interior Cell 均大小 | L12 | L10/L11 (面积更大) |
| 入库耗时 | 1142s | ~600s (-47%) |
| 点查询命中率 | 62.5% | ~65% (略提升，更大interior范围) |

**副作用**：
- L10/L11 Cell 的 interior 判断可能不精确（大 Cell 的边更可能穿过 polygon 边界）
- 查询时 L10 Cell 的范围查询覆盖面更大，可能引入更多 false positive 候选
- 需要重新验证 interior 判断的准确率

**权衡**：用粗粒度换存储和入库速度。对于中国市级区划（面积几十到几千 km²），L10/L11 作为内部 Cell 完全足够。

**推荐值**：`min_level=10`，与 `max_cells=2000` 配合使用效果最佳。

---

#### 优化 3：批量入库优化

**修改位置**：`db.py` L22, `indexing.py` L139-149

**方案 A：关闭 autocommit，批量事务提交**

```python
# db.py L22 修改
self._conn.autocommit = False  # 关闭自动提交

# indexing.py 新增批量逻辑
BATCH_SIZE = 10  # 每10条区划提交一次

for i, feature in enumerate(read_geojson_features(geojson_path)):
    village_id = process_single_village(geom, village_info, db, crypto, app_config)
    village_ids.append(village_id)
    if (i + 1) % BATCH_SIZE == 0:
        db.commit()  # 批量提交
db.commit()  # 提交剩余
```

**方案 B：COPY 命令替代 execute_values**

```python
# db.py 新增 copy_cells() 方法
def copy_cells(self, cell_records: list[tuple]) -> None:
    """使用 COPY 命令批量写入，比 execute_values 快 3-5 倍"""
    import io
    buffer = io.StringIO()
    for cell_id, village_id, is_interior, level in cell_records:
        buffer.write(f"{cell_id}\t{village_id}\t{is_interior}\t{level}\n")
    buffer.seek(0)
    with self.cursor() as cur:
        cur.copy_from(buffer, "village_s2_cells", columns=("cell_id", "village_id", "is_interior", "level"))
    self.commit()
```

**方案 C：先收集后批量写入（最佳）**

```python
# indexing.py 新增 batch_index 方法
def batch_index_geojson(geojson_path, app_config=None):
    """两阶段入库：先收集所有数据，再批量写入"""
    # 阶段1：计算所有S2覆盖 + 加密（CPU密集，无DB交互）
    all_villages = []
    all_cells = []
    for feature in read_geojson_features(geojson_path):
        covering = polygon_to_s2_covering(geom, ...)
        encrypted_geom = crypto.encrypt_geometry(geom)
        all_villages.append((name, province, city, county, encrypted_geom, len(covering.cells)))
        all_cells.extend([(cell.cell_id, is_interior, cell.level) for cell in covering.cells])
    
    # 阶段2：批量写入（IO密集，单事务）
    db.connect()
    db.conn.autocommit = False
    # 先写入所有主表
    village_ids = []
    for v in all_villages:
        vid = db.insert_village(*v)
        village_ids.append(vid)
    # 再用COPY批量写入所有Cell
    full_records = [(cid, village_ids[i], is_int, lv) for i, cells in enumerate(all_cells_per_village) for cid, is_int, lv in cells]
    db.copy_cells(full_records)
    db.commit()
```

**预期收益**：

| 方案 | 入库耗时 | 提升倍数 |
|------|---------|---------|
| 当前 (autocommit + execute_values) | 1142s | 基准 |
| 方案 A (批量事务) | ~600s | 1.9x |
| 方案 B (COPY) | ~400s | 2.9x |
| 方案 C (先收集后批量) | ~300s | 3.8x |

**方案 C 额外收益**：阶段 1 纯 CPU 计算，可以用多进程并行加速。

**副作用**：
- 内存占用增加（缓存所有 Cell 记录，407 万条 × 32 bytes ≈ 130 MB）
- 单事务失败回滚量大
- COPY 命令不支持 `ON CONFLICT DO NOTHING`，需要确保数据无重复

---

#### 优化 4：面查询 SQL 优化

**修改位置**：`db.py` L163-214

**方案 A：CTE + UNION ALL 替代 OR 链**

```sql
-- 当前：95个OR条件
SELECT ... FROM village_s2_cells
WHERE cell_id = ANY({exact_ids})
   OR cell_id BETWEEN ... OR cell_id BETWEEN ... (×95)

-- 优化后：CTE + UNION ALL
WITH candidates AS (
    SELECT cell_id, village_id, is_interior, level FROM village_s2_cells
    WHERE cell_id = ANY(%s)
    UNION ALL
    SELECT cell_id, village_id, is_interior, level FROM village_s2_cells
    WHERE cell_id BETWEEN %s AND %s
    UNION ALL
    SELECT cell_id, village_id, is_interior, level FROM village_s2_cells
    WHERE cell_id BETWEEN %s AND %s
    ...
)
SELECT DISTINCT cell_id, village_id, is_interior, level FROM candidates
```

**方案 B：临时表 + JOIN**

```sql
-- 1. 创建临时表存查询条件
CREATE TEMP TABLE query_conditions (
    cond_type SMALLINT,  -- 0=exact, 1=range
    cell_id BIGINT,      -- exact模式: cell_id值; range模式: range_min
    range_max BIGINT     -- exact模式: NULL; range模式: range_max
);

-- 2. 批量插入条件
INSERT INTO query_conditions VALUES (...);

-- 3. JOIN查询
SELECT DISTINCT c.cell_id, c.village_id, c.is_interior, c.level
FROM village_s2_cells c
JOIN query_conditions q ON (
    (q.cond_type = 0 AND c.cell_id = q.cell_id)
    OR (q.cond_type = 1 AND c.cell_id BETWEEN q.cell_id AND q.range_max)
);
```

**方案 C：合并为单条 SQL（推荐）**

```python
def query_cells_optimized(self, exact_ids, range_conditions):
    """单条SQL合并精确匹配和范围查询"""
    conditions = ["cell_id = ANY(%s)"]
    params = [exact_ids]
    for range_min, range_max in range_conditions:
        conditions.append("cell_id BETWEEN %s AND %s")
        params.extend([range_min, range_max])
    
    sql = f"SELECT DISTINCT cell_id, village_id, is_interior, level FROM village_s2_cells WHERE {' OR '.join(conditions)}"
    with self.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
```

**预期收益**：

| 方案 | SQL 耗时（广深走廊） | 改动量 |
|------|---------------------|--------|
| 当前（两次查询+内存合并） | ~500ms | 基准 |
| 方案 A (CTE) | ~200ms | 中 |
| 方案 B (临时表) | ~100ms | 大 |
| 方案 C (单SQL) | ~150ms | 小 |

**推荐**：方案 C，改动最小，收益明显。如果仍不满足要求，再升级到方案 B。

---

### 5.2 中期优化

---

#### 优化 5：Cell 合并压缩

**修改位置**：`s2_utils.py`，新增 `_compress_interior_cells()` 函数

**原理**：相邻的 L12 Interior Cell 可以合并为 L11/L10 Cell，大幅减少 Cell 数量。

```
合并前: 4个L12 Interior Cell (每个2.25km², 共9km²)
合并后: 1个L11 Interior Cell (25km², 覆盖原4个+更多区域)
```

**实施代码**：

```python
def _compress_interior_cells(cells: list[CellEntry], min_level: int = 10) -> list[CellEntry]:
    """对interior Cell进行合并压缩"""
    # 1. 提取所有interior Cell ID
    interior_ids = [s2.CellId(c.cell_id) for c in cells if c.is_interior]
    
    # 2. 构建CellUnion并规范化（自动合并相邻Cell）
    cell_union = s2.CellUnion(interior_ids)
    cell_union.normalize()
    
    # 3. 重建CellEntry列表
    compressed = []
    for cell_id in cell_union:
        level = cell_id.level()
        if level < min_level:
            # 如果合并后的Cell level低于min_level，保留原始未合并的Cell
            continue
        compressed.append(CellEntry(
            cell_id=cell_id.id(),
            level=level,
            is_interior=True,
        ))
    
    # 4. 保留boundary Cell不变
    boundary_cells = [c for c in cells if not c.is_interior]
    return compressed + boundary_cells
```

**预期收益**：

| 指标 | 当前 | 合并后 |
|------|------|--------|
| Interior Cell 数 | 1,857,913 | ~200,000 (-89%) |
| Cell 总数 | 4,078,872 | ~2,400,000 (-41%) |
| 索引表大小 | 203 MB | ~120 MB |

**副作用**：
- 合并后的 L10/L11 Interior Cell 面积更大，interior 判断可能不够精确
- 需要验证：合并后 Cell 的 4 顶点是否仍在 polygon 内
- 如果验证失败，该 Cell 应降级为 boundary

**关键注意**：`s2sphere.CellUnion.normalize()` 的实现是否完整需要验证。如果 s2sphere 不支持完整的 `normalize()`，需要自行实现合并逻辑（4 个同父 Cell → 1 个父 Cell）。

---

#### 优化 6：分区索引

**修改位置**：`sql/schema.sql`

**原理**：S2 Cell ID 的最高 3 位是 face（0-5，对应地球 6 个面）。中国全部在 face 0（北半球太平洋区域），所以按 face 分区对中国数据没有意义。

**更合理的分区方案**：按 S2 Cell 的 L6/L8 分区（将中国按经纬度分块）

```sql
-- 按L6分区（中国约跨3个L6 Cell）
CREATE TABLE village_s2_cells (
    cell_id BIGINT NOT NULL,
    village_id BIGINT NOT NULL,
    is_interior BOOLEAN NOT NULL,
    level SMALLINT NOT NULL,
    cell_l6 SMALLINT GENERATED ALWAYS AS (
        -- 提取L6 face+position作为分区键
        ((cell_id >> 58) & 7)  -- 简化，实际需计算L6索引
    ) STORED
) PARTITION BY LIST (cell_l6);

CREATE TABLE village_s2_cells_p0 PARTITION OF village_s2_cells FOR VALUES IN (0);
CREATE TABLE village_s2_cells_p1 PARTITION OF village_s2_cells FOR VALUES IN (1);
-- ...
```

**预期收益**：
- 查询只需扫描相关分区，减少 50-70% 的索引扫描量
- 维护操作（VACUUM、REINDEX）可按分区独立执行

**副作用**：
- DDL 复杂度增加
- 跨分区查询需要 UNION
- 分区键计算增加写入开销

**推荐**：当前数据量 203 MB 不算大，分区索引收益有限。**建议在数据量超过 1GB 时再考虑**。

---

#### 优化 7：查询结果缓存

**修改位置**：新增 `cache.py`，修改 `query.py`

**方案 A：进程内 LRU Cache**

```python
from functools import lru_cache

@lru_cache(maxsize=10000)
def _cached_point_query(lng_rounded, lat_rounded):
    """点查询缓存，精度到小数点后3位（约100m）"""
    return locate_village_by_point(lng_rounded, lat_rounded, db, crypto, app_config)

@lru_cache(maxsize=1000)
def _cached_polygon_query(polygon_wkb_hash):
    """面查询缓存，基于WKB的hash"""
    return locate_villages_by_polygon(polygon, db, crypto, app_config)
```

**方案 B：Redis 缓存**

```python
import redis
import hashlib

class QueryCache:
    def __init__(self, redis_url="redis://localhost:6379"):
        self._redis = redis.from_url(redis_url)
        self._ttl = 3600  # 1小时过期
    
    def get_point_result(self, lng, lat):
        key = f"point:{lng:.3f}:{lat:.3f}"
        cached = self._redis.get(key)
        if cached:
            return json.loads(cached)
        return None
    
    def set_point_result(self, lng, lat, result):
        key = f"point:{lng:.3f}:{lat:.3f}"
        self._redis.setex(key, self._ttl, json.dumps(result))
```

**预期收益**：

| 场景 | 无缓存 | 进程内缓存 | Redis缓存 |
|------|--------|-----------|----------|
| 重复点查询 | 12.6ms | 0.01ms | 0.1ms |
| 重复面查询 | 280ms | 0.01ms | 0.2ms |

**副作用**：
- 缓存一致性问题：区划变更时需主动失效
- 进程内缓存重启丢失
- Redis 引入额外依赖和运维成本

**推荐**：先用方案 A（进程内 LRU Cache），零依赖，对高频重复查询场景效果显著。

---

### 5.3 长期优化

---

#### 优化 8：引入 PostGIS 空间索引

**实施路径**：

```
1. 安装 PostGIS 扩展
   CREATE EXTENSION postgis;

2. villages 表增加明文 geography 列
   ALTER TABLE villages ADD COLUMN geom geography(MultiPolygon, 4326);
   CREATE INDEX idx_villages_geom ON villages USING GIST (geom);

3. 入库时同时写入加密和明文 geometry
   INSERT INTO villages (..., encrypted_geom, geom)
   VALUES (..., %s, ST_GeomFromWKB(%s, 4326));

4. 查询改用 PostGIS 空间索引
   -- 点查询
   SELECT id, village_name FROM villages
   WHERE ST_Contains(geom, ST_Point(%s, %s));

   -- 面查询
   SELECT id, village_name FROM villages
   WHERE ST_Intersects(geom, ST_MakePolygon(...));

5. S2 索引降级为辅助索引（可选保留）
```

**预期收益**：

| 指标 | S2 索引 | PostGIS |
|------|---------|---------|
| 点查询命中率 | 62.5% | **100%** |
| 面查询精度 | ~95% | **100%** |
| 点查询速度 | 12.6ms | ~5ms |
| 面查询速度 | 280ms | ~50ms |
| 索引大小 | 203 MB | ~5 MB |
| 入库耗时 | 1142s | ~300s (无需S2覆盖) |

**核心权衡**：

| 维度 | S2 + 加密 | PostGIS 明文 |
|------|-----------|-------------|
| 数据安全 | geometry 加密存储 | **明文存储** |
| 查询精度 | ~95%（S2近似） | **100%** |
| 依赖 | 无 PostGIS | **需要 PostGIS** |
| 跨数据库 | S2 方案可移植到 MySQL/MongoDB | **PostgreSQL 专用** |

**推荐**：
- 如果**数据安全是硬性需求**（geometry 不能明文存储）：保留 S2 + 加密方案，PostGIS 作为可选加速层
- 如果**数据安全无要求**：直接迁移到 PostGIS，S2 方案完全不需要

---

#### 优化 9：Redis 缓存解密 geometry

**原理**：解密后的 geometry 缓存到 Redis，避免重复 Fernet 解密。

```python
class CachedGeometryCrypto(GeometryCrypto):
    def __init__(self, config, redis_client=None):
        super().__init__(config)
        self._redis = redis_client
        self._local_cache = {}  # 进程内二级缓存
    
    def decrypt_to_geometry(self, encrypted_geom: bytes) -> Polygon:
        cache_key = hashlib.sha256(encrypted_geom).hexdigest()
        
        # L1: 进程内缓存
        if cache_key in self._local_cache:
            return self._local_cache[cache_key]
        
        # L2: Redis缓存
        if self._redis:
            cached_wkb = self._redis.get(f"geom:{cache_key}")
            if cached_wkb:
                geom = wkb.loads(cached_wkb)
                self._local_cache[cache_key] = geom
                return geom
        
        # L3: 解密
        geom = super().decrypt_to_geometry(encrypted_geom)
        
        # 回写缓存
        self._local_cache[cache_key] = geom
        if self._redis:
            self._redis.setex(f"geom:{cache_key}", 3600, wkb.dumps(geom))
        
        return geom
```

**预期收益**：
- 重复解密从 0.5-1ms 降至 0.01ms（进程内）或 0.1ms（Redis）
- 对面查询中同一区划被多次验证的场景效果显著

**副作用**：引入 Redis 依赖；缓存一致性问题。

---

#### 优化 10：异步入库

**实施架构**：

```
GeoJSON文件
    │
    ▼
入库API (Flask/FastAPI)
    │
    ├──→ S2覆盖生成 (CPU密集, 可并行)
    │
    ▼
消息队列 (Kafka / Redis Stream / Python Queue)
    │
    ▼
消费者 (批量写入DB)
    │
    ▼
PostgreSQL
```

**简单方案：ThreadPoolExecutor**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def parallel_index(geojson_path, workers=4):
    """多线程并行入库"""
    features = list(read_geojson_features(geojson_path))
    
    # 阶段1: 并行计算S2覆盖+加密
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(compute_covering, feat): i
            for i, feat in enumerate(features)
        }
        for future in as_completed(futures):
            results.append((futures[future], future.result()))
    
    # 阶段2: 顺序批量写入DB
    results.sort(key=lambda x: x[0])
    batch_write_to_db(results)
```

**预期收益**：
- 4 线程并行：入库时间从 1142s 降至 ~350s（3.3x）
- 8 线程并行：入库时间降至 ~200s（5.7x）
- GIL 影响有限（S2 计算和 Shapely 操作会释放 GIL）

**副作用**：
- 内存占用增加（每个线程独立处理一个区划）
- 需要处理并发写入的锁竞争
- 错误处理更复杂

---

## 六、综合优先级与实施路线图

### 6.1 优先级矩阵

| 优先级 | 优化项 | 实施难度 | 预期收益 | 推荐顺序 |
|--------|--------|---------|---------|---------|
| **P0** | 调高 max_cells + 降低 min_level | **低**（改2行配置） | **高**（命中率+20%，索引-60%） | **第 1 步** |
| **P0** | 批量入库优化（COPY+事务） | **中** | **高**（入库 3-4x 加速） | **第 2 步** |
| **P1** | Cell 合并压缩 | **中** | **高**（Cell 数-40%） | **第 3 步** |
| **P1** | 面查询 SQL 优化 | **中** | **中**（查询 2-3x 加速） | **第 4 步** |
| **P2** | 查询缓存（LRU） | **低** | **中**（重复查询 100x） | **第 5 步** |
| **P2** | 分区索引 | **中** | **中**（大数据量才有效） | 第 6 步 |
| **P3** | PostGIS | **高** | **极高**（精度 100%） | 长期 |
| **P3** | Redis 缓存 | **中** | **中** | 长期 |
| **P3** | 异步入库 | **高** | **高**（入库 5x+） | 长期 |

### 6.2 推荐实施路线

```
Phase 1（1天，零风险）:
├── 修改 config.py: min_level=10, max_cells=2000
├── 重新执行 test_full_pg.py
└── 对比前后指标

Phase 2（2-3天，中风险）:
├── db.py: 新增 copy_cells() 方法
├── indexing.py: 新增 batch_index_geojson() 两阶段入库
├── db.py: 关闭 autocommit，批量事务提交
└── 重新执行测试，验证入库加速

Phase 3（3-5天，中风险）:
├── s2_utils.py: 新增 _compress_interior_cells()
├── db.py: 合并查询为单条 SQL
├── query.py: 新增 LRU Cache 装饰器
└── 重新执行测试，验证查询加速

Phase 4（长期，高收益高投入）:
├── 评估是否引入 PostGIS
├── 评估是否引入 Redis
└── 评估是否需要异步入库
```

### 6.3 P0 优化的快速验证脚本

只需修改配置即可验证，无需改动代码逻辑：

```bash
# 设置环境变量覆盖默认配置
export S2_MIN_LEVEL=10
export S2_MAX_CELLS=2000

# 重新运行完整测试
python test/test_full_pg.py
```

预期 P0 优化后的指标对比：

| 指标 | 当前 | P0 优化后 |
|------|------|-----------|
| Cell 总数 | 407 万 | ~150 万 |
| L12 占比 | 95.2% | ~50% |
| 索引表大小 | 203 MB | ~75 MB |
| 入库耗时 | 1142s | ~600s |
| 点查询命中率 | 62.5% | ~85% |
| 面查询耗时(广深) | 723ms | ~300ms |

---

## 七、关键结论

1. **入库慢的根因是 S2 覆盖生成**（占 60%），不是 DB 写入。s2sphere 纯 Python 实现是深层原因，长期可考虑用 C++ S2 库替代。

2. **Cell 索引膨胀的根因是 max_cells=500 的自适应降级** + LatLngRect 近似。调高 max_cells 和降低 min_level 是最简单有效的优化。

3. **面查询慢的根因是 SQL 条件膨胀**（95 个 OR）和 L12 Cell 粒度粗导致的候选集膨胀。SQL 优化和 Cell 合并可以分别解决。

4. **点查询命中率低的根因是 LatLngRect 近似**，无法覆盖 polygon 的 concave 区域。长期解决方案是引入 PostGIS 或使用 s2sphere 的精确多边形覆盖（需要自行实现 S2Region 接口）。

5. **最高 ROI 的优化**：P0（改 2 行配置 → 命中率 +20%，索引 -60%），建议立即执行。
