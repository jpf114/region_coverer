-- ============================================================
-- 村落S2空间索引系统 - 数据库Schema
-- 核心设计：villages主表(加密几何只存1份) + village_s2_cells索引表(轻量)
-- ============================================================

-- 村落主表：几何数据只存一份（加密）
CREATE TABLE villages (
    id              BIGSERIAL PRIMARY KEY,
    village_name    VARCHAR(100) NOT NULL,
    province        VARCHAR(50),
    city            VARCHAR(50),
    county          VARCHAR(50),
    encrypted_geom  BYTEA NOT NULL,          -- Fernet加密的WKB几何数据
    cell_count      INTEGER DEFAULT 0,       -- 该村落的S2覆盖Cell数量（统计/调试用）
    created_at      TIMESTAMP DEFAULT NOW()
);

-- 按行政区查询村落
CREATE INDEX idx_villages_county ON villages (province, city, county);

-- ============================================================

-- S2 Cell索引表：轻量级，仅存Cell ID与关联关系
-- 核心查询入口：通过cell_id快速定位候选村落
CREATE TABLE village_s2_cells (
    cell_id         BIGINT NOT NULL,          -- S2 Cell ID (Hilbert编码，天然有序)
    village_id      BIGINT NOT NULL REFERENCES villages(id) ON DELETE CASCADE,
    is_interior     BOOLEAN NOT NULL,         -- true=完全在村落内部的Cell, false=边界Cell
    level           SMALLINT NOT NULL,        -- S2 level (12~18)
    PRIMARY KEY (cell_id, village_id)
);

-- 查询核心索引：cell_id是主查询入口
-- S2 Cell ID按Hilbert曲线编码，天然保局部性，B+树索引效率极高
CREATE INDEX idx_vsc_cell ON village_s2_cells (cell_id);
CREATE INDEX idx_vsc_village ON village_s2_cells (village_id);

-- 按level筛选（调试/统计用）
CREATE INDEX idx_vsc_level ON village_s2_cells (level);
