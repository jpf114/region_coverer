# 村落S2空间索引查询系统

基于S2 Geometry + Fernet加密的村落空间索引与查询系统。支持**面矢量查询**（输入Polygon，返回相交的村落列表）和**点查询**（输入经纬度，返回所属村落）。

## 架构概览

```
┌──────────────┐     入库管道           ┌──────────────┐
│  村落矢量数据 │ ───────────────────→  │  数据库存储   │
│  (GeoJSON)   │  S2覆盖+加密geometry  │  villages    │
└──────────────┘                       │  village_s2  │
                                       └──────┬───────┘
                                              │
┌──────────────┐     查询服务           ┌──────┴───────┐
│  输入面/点    │ ───────────────────→  │  S2粗过滤    │
│              │  返回村落名称列表      │  → 解密验证   │
└──────────────┘                       └──────────────┘
```

## 核心特性

- **面矢量查询**：输入Polygon，返回所有相交的村落名称
- **点查询**：输入经纬度，返回所属村落（逐级回溯18→12，修复原代码只查level=18的缺陷）
- **自适应S2覆盖**：村落内部大Cell(level 12)、边界小Cell(level 18)
- **内部/边界区分**：interior Cell命中可跳过解密验证，大幅减少开销
- **加密数据保护**：geometry Fernet加密存储，仅按需解密验证
- **毫秒级响应**：S2 Cell ID Hilbert有序编码，B+树索引高效
- **独立数据库**：使用 `region_coverer` 独立库，不污染默认 `postgres` 库

## 目录结构

```
├── src/
│   ├── __init__.py
│   ├── config.py         # 全局配置：S2参数、DB连接、加密密钥
│   ├── crypto.py         # Fernet加密封装
│   ├── s2_utils.py       # S2工具：覆盖生成、祖先展开、范围查询
│   ├── db.py             # 数据库封装：连接管理、批量写入、查询
│   ├── indexing.py       # 入库管道：GeoJSON→S2覆盖→加密→写入
│   └── query.py          # 查询服务：面查询+点查询
├── sql/
│   └── schema.sql        # PostgreSQL建表SQL
├── test/
│   ├── china.geojson     # 中国市级区划矢量数据（477个面要素）
│   ├── pg.ncx            # Navicat PostgreSQL连接配置
│   ├── test_full_pg.py   # 完整测试脚本（建库→建表→入库→查询→统计）
│   ├── test_key.bin      # Fernet加密密钥文件
│   └── TEST_REPORT.md    # 测试报告文档
├── tests/
│   └── test_e2e.py       # 端到端测试（SQLite离线运行）
├── requirements.txt
└── README.md
```

## 快速开始

### 环境要求

- Python 3.10+（推荐 Conda GIS 环境）
- PostgreSQL 12+

### 安装依赖

```bash
pip install -r requirements.txt
```

依赖列表：

| 包 | 用途 |
|----|------|
| s2sphere | S2几何库，Cell覆盖与索引 |
| shapely | 矢量几何操作（相交判断、WKB序列化） |
| cryptography | Fernet对称加密 |
| psycopg2-binary | PostgreSQL驱动 |

### 配置

通过环境变量配置：

```bash
# S2参数（可选，有默认值）
export S2_MIN_LEVEL=12    # 内部最大Cell级别，≈1.5km
export S2_MAX_LEVEL=18    # 边界最小Cell级别，≈20m
export S2_MAX_CELLS=500   # 每个区划最多生成的Cell数

# 数据库连接
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=region_coverer   # 独立数据库名
export DB_USER=postgres
export DB_PASSWORD=your_password

# 加密密钥（二选一）
export CRYPTO_KEY_PATH=/path/to/keyfile     # 密钥文件路径
export CRYPTO_KEY_BASE64=your_base64_key    # 或直接提供Base64密钥
```

### 生成加密密钥

```python
from src.crypto import GeometryCrypto
key = GeometryCrypto.generate_key()
GeometryCrypto.save_key(key, "secret.key")
# 然后设置 CRYPTO_KEY_PATH=secret.key
```

### 初始化数据库

```bash
# 方式一：手动建库建表
createdb -U postgres region_coverer
psql -U postgres -d region_coverer -f sql/schema.sql

# 方式二：运行完整测试脚本（自动建库建表入库）
python test/test_full_pg.py
```

### 入库

```python
from src.indexing import index_geojson_file
village_ids = index_geojson_file("villages.geojson")
```

### 查询

```python
from src.query import query_villages_by_polygon, query_village_by_point
from shapely.geometry import Polygon

# 面矢量查询：输入Polygon → 返回所有相交区划
poly = Polygon([(116.3, 39.9), (116.5, 39.9), (116.5, 40.0), (116.3, 40.0)])
results = query_villages_by_polygon(poly)
for r in results:
    print(f"  {r.village_name} ({r.province}/{r.city}/{r.county})")

# 点查询：输入经纬度 → 返回所属区划
result = query_village_by_point(116.39, 39.90)
if result:
    print(f"  所属区划: {result.village_name}")
```

### 运行测试

```bash
# SQLite离线测试（无需PostgreSQL）
python tests/test_e2e.py

# PostgreSQL完整测试（使用china.geojson 477条数据）
python test/test_full_pg.py
```

## 数据库设计

### villages（主表）

| 列名 | 类型 | 说明 |
|------|------|------|
| id | BIGSERIAL PK | 自增主键 |
| village_name | VARCHAR(100) | 区划名称 |
| province | VARCHAR(50) | 省份 |
| city | VARCHAR(50) | 城市 |
| county | VARCHAR(50) | 区县 |
| encrypted_geom | BYTEA | Fernet加密的WKB几何数据（仅存1份） |
| cell_count | INTEGER | S2覆盖Cell数量 |
| created_at | TIMESTAMP | 创建时间 |

### village_s2_cells（S2索引表）

| 列名 | 类型 | 说明 |
|------|------|------|
| cell_id | BIGINT | S2 Cell ID（Hilbert编码，天然有序） |
| village_id | BIGINT FK | 关联villages.id |
| is_interior | BOOLEAN | true=内部Cell(跳过解密), false=边界Cell |
| level | SMALLINT | S2 level (12~18) |

索引：
- `PRIMARY KEY (cell_id, village_id)` — 防止重复
- `idx_vsc_cell (cell_id)` — 核心查询入口
- `idx_vsc_village (village_id)` — 按村落查询
- `idx_vsc_level (level)` — 统计调试

## 核心算法

### 面矢量查询算法（3层过滤）

```
输入Polygon
    │
    ▼
第1层：S2 Cell粗过滤（明文索引，毫秒级）
    │ 为输入Polygon生成S2覆盖
    │ 展开祖先Cell + 范围查询 → 找到所有候选Cell
    │
    ▼
第2层：候选去重与分类
    │ 提取去重的village_id集合
    │ interior Cell命中 → 直接确认（无需解密！）
    │ boundary Cell命中 → 加入待验证集合
    │
    ▼
第3层：解密+几何精确验证（仅boundary候选）
    │ 解密village的geometry → Shapely intersects()验证
    │
    ▼
返回确认相交的村落名称列表
```

### 点查询算法（逐级回溯）

```
输入(lng, lat)
    │
    ▼
转换为S2 Leaf Cell (level 30)
    │
    ▼
从level=18逐级向上回溯到level=12
    │ level=18 → level=17 → ... → level=12
    │ 批量查询所有祖先Cell
    │
    ▼
优先检查高level（精确匹配）
    │ 命中interior Cell → 直接返回（无需解密）
    │ 命中boundary Cell → 解密验证
    │
    ▼
返回所属村落 或 None
```

## 测试结果摘要

> 详见 [test/TEST_REPORT.md](test/TEST_REPORT.md)

### 测试环境

- PostgreSQL 16.13，数据库 `region_coverer`
- 数据源：china.geojson（477个中国市级区划）
- S2参数：min_level=12, max_level=18, max_cells=500

### 入库统计

| 指标 | 数值 |
|------|------|
| 入库区划数 | 477 |
| S2 Cell 总数 | 4,078,872 |
| Interior Cell | 1,857,913 (45.6%) |
| Boundary Cell | 2,220,959 (54.4%) |
| 入库耗时 | 1,142s (~19min) |
| 加密geom总大小 | 3,491 KB |
| Cell索引表大小 | 203 MB |

### 查询性能

| 查询类型 | 平均耗时 | 测试结果 |
|----------|----------|----------|
| 点查询 | 12.6ms | 5/8 命中 |
| 面查询(小区域) | ~90ms | 全部PASS |
| 面查询(大区域) | ~500ms | 全部PASS |

### S2 Level分布

| Level | Cell数 | 占比 |
|-------|--------|------|
| 12 | 3,884,634 | 95.2% |
| 13~18 | 194,238 | 4.8% |

> Level 12 占比过高，建议调高 max_cells 至 2000~5000 或降低 min_level 至 10。

## 与原代码对比

| 维度 | 原代码 | 重构后 |
|------|--------|--------|
| geometry存储 | 每个Cell行存一份加密geometry | 主表仅存1份，消除冗余 |
| 点查询 | 只查level=18单一Cell（漏查内部大Cell） | 逐级回溯18→12 |
| is_interior | 无 | 有，可跳过内部Cell解密验证 |
| 面矢量查询 | 不支持 | 完整支持（3层过滤） |
| S2 max_cells | 20（远远不够） | 500（合理覆盖） |
| 加密密钥 | 硬编码generate_key() | 环境变量/配置文件 |
| 数据库 | postgres默认库 | 独立region_coverer库 |
| BYTEA兼容 | 未处理memoryview | bytes()转换兼容 |

## 优化建议

1. **调高 max_cells**：从 500 提升至 2000~5000，减少 L12 粗粒度 Cell 占比
2. **降低 min_level**：从 12 降至 10，允许更大的内部 Cell
3. **批量入库优化**：使用 COPY 代替 executemany，预计提升 3~5 倍
4. **Cell 合并**：对 L12 interior Cell 合并为 L10/L11 Cell，减少索引膨胀
5. **引入 PostGIS**：用空间索引替代 S2 粗过滤，提升查询精度和命中率
6. **内存缓存层**：Redis 缓存热点区划的解密 geometry，减少重复解密
