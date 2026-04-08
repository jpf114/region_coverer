---
name: region_coverer_polygon_query
overview: 基于现有加密数据环境，设计完整的面矢量→村落查询解决方案。包含：Schema 重构（消除冗余加密、增加 is_interior/level 标记）、S2 覆盖生成优化（内外覆盖分离、参数调优）、面矢量查询算法（S2 cell 相交过滤 + 解密精确验证）、加密流程优化、接口设计及性能评估。
todos:
  - id: schema-design
    content: 创建sql/schema.sql，设计villages主表与village_s2_cells索引表的分离Schema
    status: completed
  - id: config-crypto
    content: 创建src/config.py和src/crypto.py，实现配置管理与Fernet加密封装
    status: completed
  - id: s2-utils
    content: 创建src/s2_utils.py，实现S2覆盖生成、内部/边界Cell区分、祖先展开与范围查询工具函数
    status: completed
    dependencies:
      - config-crypto
  - id: db-layer
    content: 创建src/db.py，实现数据库连接池、批量写入与查询辅助方法
    status: completed
    dependencies:
      - schema-design
  - id: indexing-pipeline
    content: 创建src/indexing.py，实现村落矢量入库管道：GeoJSON读取→S2覆盖→加密geometry→批量写入
    status: completed
    dependencies:
      - s2-utils
      - db-layer
  - id: query-service
    content: 创建src/query.py，实现面矢量查询(locate_villages_by_polygon)与修复后的点查询(locate_village_by_point)
    status: completed
    dependencies:
      - s2-utils
      - db-layer
  - id: integration-test
    content: 创建端到端测试，用示例Polygon验证面查询与点查询的正确性
    status: completed
    dependencies:
      - indexing-pipeline
      - query-service
---

## 用户需求

基于现有的加密数据环境（Fernet对称加密几何数据WKB），设计一个完整的面矢量查询村落系统：输入一个面矢量（Polygon），精确返回该面矢量所占据（相交）的村落名称列表。

## 产品概述

系统分为两大阶段：**数据入库管道**（村落矢量 → S2 Cell覆盖 → 加密存储）和**查询服务**（输入面矢量 → S2 Cell过滤 → 解密验证 → 返回村落名称）。所有几何数据以加密形式存储，仅S2 Cell ID为明文索引用于快速过滤。

## 核心特性

- **面矢量查询**：输入Polygon，返回所有相交的村落名称列表
- **点查询（兼容）**：输入经纬度点，返回所属村落名称
- **自适应S2覆盖**：村落内部用大Cell、边界用小Cell，兼顾存储效率与查询精度
- **加密数据保护**：几何数据Fernet加密存储，仅按需解密验证
- **内部/边界Cell区分**：内部Cell命中可跳过几何验证，减少解密开销
- **查询性能**：毫秒级响应，S2 Cell ID的Hilbert有序性保证B+树索引高效

## 现有代码关键缺陷（需修复）

1. 查询逻辑只查level=18单一Cell，内部大Cell完全无法命中
2. 无is_interior标记，每次查询都要解密+几何验证
3. encrypted_geom在每个Cell行中重复存储，同一村落几何被加密N次
4. S2参数max_cells=20对村落覆盖远远不够
5. 加密密钥硬编码，无安全管理

## 技术栈

- **语言**: Python 3.10+
- **S2库**: s2sphere（Google S2 Python移植版）
- **几何计算**: Shapely（多边形处理、相交判断）
- **加密**: cryptography.fernet（对称加密）
- **数据库**: PostgreSQL + psycopg2
- **数据格式**: GeoJSON输入，WKB内部序列化

## 实现方案

### 核心算法：面矢量查询村落

面矢量与村落的相交检测分三层：

**第一层：S2 Cell粗过滤（明文索引，毫秒级）**

- 将输入Polygon生成S2 Cell覆盖
- 对每个输入Cell，展开其所有祖先Cell（从min_level到当前level）
- 同时利用S2 Cell的range_min/range_max特性，检测数据库中属于输入Cell子树的所有后代Cell
- 两条路径合并确保：大Cell覆盖小Cell、小Cell属于大Cell的子树均不遗漏

**第二层：候选去重与分类**

- 从粗过滤结果中提取去重的village_id集合
- 判断候选村落是否全部由interior Cell命中（可跳过精确验证）

**第三层：解密+几何精确验证（仅边界候选）**

- 对需要验证的候选村落，从villages主表读取加密geometry
- Fernet解密得到WKB → Shapely反序列化
- 使用Shapely `intersects()` 判断输入面与村落面是否真正相交

### S2 Cell相交检测的数学保证

```
两个Polygon相交 ⟺ 它们的S2覆盖共享至少一个叶Cell
                                    ↓ 等价于
覆盖Cell集合中存在: Cell_A == Cell_B 或 Cell_A是Cell_B的祖先/后代
                                    ↓ 查询实现
1. 祖先展开: 输入Cell的所有祖先ID → 等值匹配
2. 范围查询: 输入Cell的[range_min, range_max] → 捕获后代Cell
```

### 数据库Schema重构

**核心变更：主表与索引表分离，geometry只加密存储一次**

```sql
-- 村落主表：几何数据只存一份
CREATE TABLE villages (
    id              BIGSERIAL PRIMARY KEY,
    village_name    VARCHAR(100) NOT NULL,
    province        VARCHAR(50),
    city            VARCHAR(50),
    county          VARCHAR(50),
    encrypted_geom  BYTEA NOT NULL,          -- Fernet加密的WKB几何
    cell_count      INTEGER DEFAULT 0,       -- 覆盖Cell数量（统计用）
    created_at      TIMESTAMP DEFAULT NOW()
);

-- S2 Cell索引表：轻量级，仅存Cell ID与关联关系
CREATE TABLE village_s2_cells (
    cell_id         BIGINT NOT NULL,          -- S2 Cell ID (Hilbert编码，天然有序)
    village_id      BIGINT NOT NULL REFERENCES villages(id),
    is_interior     BOOLEAN NOT NULL,         -- true=完全在村落内部, false=边界Cell
    level           SMALLINT NOT NULL,        -- S2 level (12~18)
    PRIMARY KEY (cell_id, village_id)
);

-- 查询核心索引
CREATE INDEX idx_vsc_cell ON village_s2_cells (cell_id);
CREATE INDEX idx_vsc_village ON village_s2_cells (village_id);
```

**对比现有设计的改进**：

| 维度 | 现有设计 | 重构后 |
| --- | --- | --- |
| geometry存储 | 每个Cell行存一份加密geometry | 主表仅存1份 |
| 存储量 | 60万村 × 200cell × 加密几何 ≈ 数百GB | 60万 × 1份加密几何 + 1.2亿轻量索引行 |
| 解密次数 | 每个候选Cell都解密 | 每个候选村落仅解密1次 |
| is_interior | 无 | 有，可跳过内部Cell的几何验证 |


### 查询流程对比

**面查询（新增）**：

```
输入Polygon → S2 Covering(100~500 cells)
  → 祖先展开 + 范围查询 → SQL粗过滤
  → 去重village_id → 分类(interior/边界)
  → interior候选直接纳入 / 边界候选解密验证
  → 返回村落名称列表
```

**点查询（修复）**：

```
输入(lng,lat) → S2 Leaf Cell(level 30)
  → 从level 18逐级向上查到level 12
  → 首次命中interior Cell → 直接返回
  → 命中边界Cell → 解密验证
  → 全部未命中 → 返回None
```

### 性能评估

| 操作 | 复杂度 | 预估耗时 |
| --- | --- | --- |
| 输入Polygon → S2 Covering | O(polygon复杂度) | 1~5ms |
| SQL粗过滤(单次批量查询) | O(log N × Cell数) | 0.5~2ms |
| 候选去重+分类 | O(候选数) | <0.1ms |
| 解密+几何验证(仅边界) | O(边界候选数) | 0.5~3ms/个 |
| **总计(典型场景)** |  | **2~10ms** |


### 实现注意事项

- **S2参数调优**: min_level=12, max_level=18, max_cells=500。max_cells=20远远不够（现有代码的缺陷），典型村落面积3~10km²，需要200~800个Cell才能充分覆盖
- **s2sphere API注意**: `init_loop`接口不支持带洞多边形(MultiPolygon)，需拆分外环和内环分别处理
- **Fernet密钥管理**: 从环境变量或配置文件读取，不再硬编码`generate_key()`
- **范围查询优化**: 对低level(大区域)输入Cell才做range查询，高level Cell只做祖先展开+等值匹配，减少SQL条件数
- **批量入库性能**: 使用COPY或executemany批量写入，单条INSERT在大数据量下极慢
- **几何验证缓存**: 对同一查询中多次出现的村落，避免重复解密

## 目录结构

```
d:/Code/MyProject/region_coverer_test/
├── src/
│   ├── __init__.py           # [NEW] 包初始化
│   ├── config.py             # [NEW] 全局配置：S2参数(min/max_level, max_cells)、DB连接串、加密密钥路径
│   ├── crypto.py             # [NEW] Fernet加密封装：encrypt_geometry/decrypt_geometry，密钥从配置加载
│   ├── s2_utils.py           # [NEW] S2工具集：polygon_to_s2_covering/polygon_to_s2_interior/expand_ancestors/get_cell_range
│   ├── indexing.py           # [NEW] 入库管道：read_geojson→generate_covering→encrypt→batch_write，支持Shapefile/GeoJSON输入
│   ├── query.py              # [NEW] 查询服务：locate_village_by_point(点查询) + locate_villages_by_polygon(面查询)
│   └── db.py                 # [NEW] 数据库封装：连接池、批量写入、查询辅助
├── sql/
│   └── schema.sql            # [NEW] 建表SQL：villages主表 + village_s2_cells索引表 + 索引
├── test/                     # [EXISTING] 保留原有测试文件作为参考
│   ├── deepseek_bash_20260408_3bbf21.sh
│   ├── deepseek_python_20260408_410ad7.py
│   ├── deepseek_python_20260408_6a99ba.py
│   └── deepseek_sql_20260408_41c16b.sql
├── requirements.txt          # [NEW] Python依赖：s2sphere, shapely, cryptography, psycopg2-binary
└── README.md                 # [NEW] 项目说明、使用方法、架构说明
```

## 关键代码结构

### S2工具核心接口 (s2_utils.py)

```python
@dataclass
class S2CoveringResult:
    """单个村落的S2覆盖结果"""
    cells: list[CellEntry]          # 所有覆盖Cell

@dataclass
class CellEntry:
    cell_id: int                    # S2 Cell ID (int64)
    level: int                      # S2 level
    is_interior: bool               # 是否完全在村落内部

def polygon_to_s2_covering(
    polygon: shapely.Geometry,
    min_level: int = 12,
    max_level: int = 18,
    max_cells: int = 500
) -> S2CoveringResult:
    """为Shapely多边形生成自适应S2覆盖（外覆盖+内覆盖区分）"""

def expand_cell_ancestors(cell_id: int, min_level: int = 12) -> list[int]:
    """展开一个S2 Cell的所有祖先Cell ID（用于查询时的粗过滤）"""

def get_cell_range(cell_id: int) -> tuple[int, int]:
    """获取S2 Cell的ID范围[min, max]，用于捕获后代Cell"""
```

### 查询服务核心接口 (query.py)

```python
@dataclass
class VillageResult:
    village_id: int
    village_name: str
    province: str
    city: str
    county: str

def locate_villages_by_polygon(
    polygon: shapely.Geometry,
    db_conn
) -> list[VillageResult]:
    """面矢量查询：返回与输入Polygon相交的所有村落"""

def locate_village_by_point(
    lng: float, lat: float, db_conn
) -> VillageResult | None:
    """点查询：返回经纬度所属村落（逐级向上回溯，修复原有缺陷）"""
```

## Agent Extensions

### SubAgent

- **code-explorer**: 用于深入搜索项目中可能存在的其他配置文件、依赖定义或数据样例，确保计划覆盖所有相关文件