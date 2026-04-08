"""
端到端测试：验证面矢量查询与点查询的正确性

测试流程：
1. 生成示例村落GeoJSON数据
2. 入库（S2覆盖 + 加密geometry + 写入SQLite）
3. 点查询验证
4. 面矢量查询验证

注意：为便于离线测试，使用SQLite替代PostgreSQL。
      生产环境请使用PostgreSQL，SQL略有差异。
"""
import os
import sys
import sqlite3
import tempfile
import json

# 确保src目录可导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shapely.geometry import Polygon, Point, MultiPolygon, mapping
from shapely import wkb

from src.config import AppConfig, S2Config, DBConfig, CryptoConfig
from src.crypto import GeometryCrypto
from src.s2_utils import (
    polygon_to_s2_covering,
    point_to_s2_cell_id,
    expand_cell_ancestors,
    get_cell_range,
    build_query_conditions,
    polygon_to_query_cells,
    CellEntry,
)
from src.indexing import process_single_village, extract_village_info


# ========== 测试数据 ==========

# 村落A：北京天安门附近矩形
VILLAGE_A = Polygon([
    (116.38, 39.89), (116.42, 39.89),
    (116.42, 39.92), (116.38, 39.92),
    (116.38, 39.89),
])

# 村落B：与村落A部分重叠（东侧）
VILLAGE_B = Polygon([
    (116.40, 39.89), (116.44, 39.89),
    (116.44, 39.92), (116.40, 39.92),
    (116.40, 39.89),
])

# 村落C：独立区域（不与A、B重叠）
VILLAGE_C = Polygon([
    (116.50, 39.89), (116.54, 39.89),
    (116.54, 39.92), (116.50, 39.92),
    (116.50, 39.89),
])

VILLAGES = [
    {"geom": VILLAGE_A, "name": "天安村", "province": "北京", "city": "北京", "county": "东城"},
    {"geom": VILLAGE_B, "name": "建國村", "province": "北京", "city": "北京", "county": "东城"},
    {"geom": VILLAGE_C, "name": "朝阳村", "province": "北京", "city": "北京", "county": "朝阳"},
]


# ========== SQLite替代数据库 ==========

class SQLiteDatabase:
    """SQLite替代PostgreSQL，用于离线测试"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = None

    def connect(self):
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def close(self):
        if self._conn:
            self._conn.close()

    @property
    def conn(self):
        return self._conn

    def commit(self):
        self._conn.commit()

    def _create_tables(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS villages (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                village_name    TEXT NOT NULL,
                province        TEXT,
                city            TEXT,
                county          TEXT,
                encrypted_geom  BLOB NOT NULL,
                cell_count      INTEGER DEFAULT 0,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS village_s2_cells (
                cell_id         INTEGER NOT NULL,
                village_id      INTEGER NOT NULL REFERENCES villages(id) ON DELETE CASCADE,
                is_interior     INTEGER NOT NULL,  -- 0=false, 1=true
                level           INTEGER NOT NULL,
                PRIMARY KEY (cell_id, village_id)
            );

            CREATE INDEX IF NOT EXISTS idx_vsc_cell ON village_s2_cells (cell_id);
            CREATE INDEX IF NOT EXISTS idx_vsc_village ON village_s2_cells (village_id);
        """)
        self._conn.commit()

    def insert_village_with_cells(self, village_name, encrypted_geom, cell_records,
                                   province=None, city=None, county=None):
        cur = self._conn.cursor()
        cur.execute(
            """INSERT INTO villages (village_name, province, city, county, encrypted_geom, cell_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (village_name, province, city, county, encrypted_geom, len(cell_records)),
        )
        village_id = cur.lastrowid

        if cell_records:
            rows = [
                (cell_id, village_id, 1 if is_interior else 0, level)
                for cell_id, is_interior, level in cell_records
            ]
            self._conn.executemany(
                """INSERT OR IGNORE INTO village_s2_cells (cell_id, village_id, is_interior, level)
                   VALUES (?, ?, ?, ?)""",
                rows,
            )
        self._conn.commit()
        return village_id

    def query_cells_by_ids(self, cell_ids):
        if not cell_ids:
            return []
        placeholders = ",".join("?" * len(cell_ids))
        cur = self._conn.execute(
            f"SELECT cell_id, village_id, is_interior, level FROM village_s2_cells WHERE cell_id IN ({placeholders})",
            cell_ids,
        )
        return [(r[0], r[1], bool(r[2]), r[3]) for r in cur.fetchall()]

    def query_cells_by_ranges(self, range_conditions):
        if not range_conditions:
            return []
        conditions = " OR ".join(["(cell_id BETWEEN ? AND ?)" for _ in range_conditions])
        params = []
        for rmin, rmax in range_conditions:
            params.extend([rmin, rmax])
        cur = self._conn.execute(
            f"SELECT cell_id, village_id, is_interior, level FROM village_s2_cells WHERE {conditions}",
            params,
        )
        return [(r[0], r[1], bool(r[2]), r[3]) for r in cur.fetchall()]

    def query_cells_by_exact_and_range(self, exact_ids, range_conditions):
        results = {}
        if exact_ids:
            for row in self.query_cells_by_ids(exact_ids):
                results[row[0]] = row
        if range_conditions:
            for row in self.query_cells_by_ranges(range_conditions):
                results[row[0]] = row
        return list(results.values())

    def get_village_by_id(self, village_id):
        cur = self._conn.execute(
            "SELECT id, village_name, province, city, county, cell_count, created_at FROM villages WHERE id = ?",
            (village_id,),
        )
        return cur.fetchone()

    def get_encrypted_geom(self, village_id):
        cur = self._conn.execute(
            "SELECT encrypted_geom FROM villages WHERE id = ?",
            (village_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_villages_by_ids(self, village_ids):
        if not village_ids:
            return {}
        placeholders = ",".join("?" * len(village_ids))
        cur = self._conn.execute(
            f"SELECT id, village_name, province, city, county FROM villages WHERE id IN ({placeholders})",
            village_ids,
        )
        return {row[0]: row for row in cur.fetchall()}

    def get_encrypted_geoms_batch(self, village_ids):
        if not village_ids:
            return {}
        placeholders = ",".join("?" * len(village_ids))
        cur = self._conn.execute(
            f"SELECT id, encrypted_geom FROM villages WHERE id IN ({placeholders})",
            village_ids,
        )
        return {row[0]: row[1] for row in cur.fetchall()}


# ========== 测试用例 ==========

def test_s2_covering():
    """测试S2覆盖生成与内部/边界区分"""
    print("\n" + "=" * 60)
    print("测试1: S2覆盖生成")
    print("=" * 60)

    result = polygon_to_s2_covering(VILLAGE_A, min_level=12, max_level=18, max_cells=500)

    print(f"  村落A覆盖: 总计{len(result.cells)}个Cell")
    print(f"    内部Cell: {len(result.interior_cells)}个")
    print(f"    边界Cell: {len(result.boundary_cells)}个")

    # 按level统计
    level_stats = {}
    for cell in result.cells:
        level_stats[cell.level] = level_stats.get(cell.level, 0) + 1
    print(f"    按level分布: {dict(sorted(level_stats.items()))}")

    assert len(result.cells) > 0, "应生成至少1个Cell"
    assert len(result.interior_cells) > 0, "应有内部Cell"
    assert len(result.boundary_cells) > 0, "应有边界Cell"
    print("  PASS S2覆盖生成测试通过")


def test_ancestor_expansion():
    """测试祖先展开"""
    print("\n" + "=" * 60)
    print("测试2: 祖先展开")
    print("=" * 60)

    # 取天安门附近的一个点
    cell_id = point_to_s2_cell_id(39.90, 116.39, level=18)
    ancestors = expand_cell_ancestors(cell_id, min_level=12)

    print(f"  点(116.39, 39.90) level=18 Cell ID: {cell_id}")
    print(f"  祖先展开(12~18): {len(ancestors)}个")

    # 验证包含自身和各级parent
    assert len(ancestors) == 7, f"从level 18到12应有7个祖先, 实际{len(ancestors)}"

    # 验证每个祖先的level
    import s2sphere as s2
    levels = [s2.CellId(a).level() for a in ancestors]
    print(f"  祖先level: {levels}")
    assert levels == [18, 17, 16, 15, 14, 13, 12], f"level序列应为[18..12], 实际{levels}"
    print("  PASS 祖先展开测试通过")


def test_cell_range():
    """测试Cell范围查询"""
    print("\n" + "=" * 60)
    print("测试3: Cell范围查询")
    print("=" * 60)

    cell_id = point_to_s2_cell_id(39.90, 116.39, level=12)
    range_min, range_max = get_cell_range(cell_id)

    print(f"  level=12 Cell ID: {cell_id}")
    print(f"  range_min: {range_min}")
    print(f"  range_max: {range_max}")
    print(f"  范围跨度: {range_max - range_min}")

    # range_min < cell_id < range_max
    assert range_min <= cell_id <= range_max, "Cell ID应在自身范围内"
    print("  PASS Cell范围查询测试通过")


def test_encrypt_decrypt():
    """测试加密/解密流程"""
    print("\n" + "=" * 60)
    print("测试4: 加密/解密流程")
    print("=" * 60)

    # 生成临时密钥
    key = GeometryCrypto.generate_key()
    with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as f:
        f.write(key)
        key_path = f.name

    try:
        crypto_config = CryptoConfig(key_path=key_path)
        crypto = GeometryCrypto(crypto_config)

        # 加密 → 解密 → 验证
        encrypted = crypto.encrypt_geometry(VILLAGE_A)
        decrypted = crypto.decrypt_to_geometry(encrypted)

        print(f"  原始Polygon: {VILLAGE_A.wkt[:50]}...")
        print(f"  加密后大小: {len(encrypted)} bytes")
        print(f"  解密后equals: {VILLAGE_A.equals(decrypted)}")

        assert VILLAGE_A.equals(decrypted), "解密后应与原始几何一致"
        print("  PASS 加密/解密流程测试通过")
    finally:
        os.unlink(key_path)


def test_end_to_end():
    """端到端测试：入库 + 点查询 + 面查询"""
    print("\n" + "=" * 60)
    print("测试5: 端到端测试（入库 + 查询）")
    print("=" * 60)

    # 生成临时密钥和数据库
    key = GeometryCrypto.generate_key()
    with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as f:
        f.write(key)
        key_path = f.name

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # 初始化
        crypto_config = CryptoConfig(key_path=key_path)
        crypto = GeometryCrypto(crypto_config)
        db = SQLiteDatabase(db_path)
        db.connect()

        app_config = AppConfig(
            s2=S2Config(min_level=12, max_level=18, max_cells=500),
        )

        # ---- 入库 ----
        print("\n  --- 入库阶段 ---")
        village_ids = []
        for v in VILLAGES:
            encrypted_geom = crypto.encrypt_geometry(v["geom"])
            covering = polygon_to_s2_covering(
                v["geom"],
                min_level=app_config.s2.min_level,
                max_level=app_config.s2.max_level,
                max_cells=app_config.s2.max_cells,
            )
            cell_records = [
                (cell.cell_id, cell.is_interior, cell.level)
                for cell in covering.cells
            ]
            vid = db.insert_village_with_cells(
                village_name=v["name"],
                encrypted_geom=encrypted_geom,
                cell_records=cell_records,
                province=v["province"],
                city=v["city"],
                county=v["county"],
            )
            village_ids.append(vid)
            print(f"    村落[{v['name']}]入库: ID={vid}, {len(covering.cells)}个Cell "
                  f"(内部{len(covering.interior_cells)}, 边界{len(covering.boundary_cells)})")

        # ---- 点查询测试 ----
        print("\n  --- 点查询阶段 ---")

        # 测试1: 点在天安村内部
        test_point_1 = (116.40, 39.905)  # 天安村内部点
        leaf_cell_id = point_to_s2_cell_id(test_point_1[1], test_point_1[0], level=18)
        ancestor_ids = expand_cell_ancestors(leaf_cell_id, min_level=12)
        cell_rows = db.query_cells_by_ids(ancestor_ids)

        print(f"    点{test_point_1}: 查到{len(cell_rows)}个匹配Cell")
        found_village = None
        boundary_candidates = set()
        for cell_id, village_id, is_interior, level in cell_rows:
            if is_interior:
                village_row = db.get_village_by_id(village_id)
                found_village = village_row[1]  # village_name
                break
            else:
                boundary_candidates.add(village_id)

        if not found_village and boundary_candidates:
            query_pt = Point(test_point_1)
            for vid in boundary_candidates:
                enc = db.get_encrypted_geom(vid)
                geom = crypto.decrypt_to_geometry(enc)
                if geom.contains(query_pt):
                    row = db.get_village_by_id(vid)
                    found_village = row[1]
                    break

        print(f"    点{test_point_1} → 村落: {found_village}")
        assert found_village == "天安村", f"期望'天安村', 实际'{found_village}'"

        # 测试2: 点在朝阳村
        test_point_2 = (116.52, 39.905)
        leaf_cell_id = point_to_s2_cell_id(test_point_2[1], test_point_2[0], level=18)
        ancestor_ids = expand_cell_ancestors(leaf_cell_id, min_level=12)
        cell_rows = db.query_cells_by_ids(ancestor_ids)

        found_village = None
        boundary_candidates = set()
        for cell_id, village_id, is_interior, level in cell_rows:
            if is_interior:
                village_row = db.get_village_by_id(village_id)
                found_village = village_row[1]
                break
            else:
                boundary_candidates.add(village_id)

        if not found_village and boundary_candidates:
            query_pt = Point(test_point_2)
            for vid in boundary_candidates:
                enc = db.get_encrypted_geom(vid)
                geom = crypto.decrypt_to_geometry(enc)
                if geom.contains(query_pt):
                    row = db.get_village_by_id(vid)
                    found_village = row[1]
                    break

        print(f"    点{test_point_2} → 村落: {found_village}")
        assert found_village == "朝阳村", f"期望'朝阳村', 实际'{found_village}'"

        # ---- 面矢量查询测试 ----
        print("\n  --- 面矢量查询阶段 ---")

        # 测试1: 查询与村落A相交的村落（应返回天安村和建國村）
        query_poly = VILLAGE_A  # 与A完全重叠，与B部分重叠
        query_cells = polygon_to_query_cells(query_poly, min_level=12, max_level=18, max_cells=100)
        exact_ids, range_conditions = build_query_conditions(query_cells, min_level=12)
        candidate_cells = db.query_cells_by_exact_and_range(exact_ids, range_conditions)

        # 去重和分类
        village_hits = {}
        for cell_id, village_id, is_interior, level in candidate_cells:
            if village_id not in village_hits:
                village_hits[village_id] = {"has_interior": False}
            if is_interior:
                village_hits[village_id]["has_interior"] = True

        confirmed_ids = set()
        verify_ids = set()
        for vid, hits in village_hits.items():
            if hits["has_interior"]:
                confirmed_ids.add(vid)
            else:
                verify_ids.add(vid)

        # 精确验证
        from shapely.prepared import prep
        prepared_input = prep(query_poly)
        if verify_ids:
            enc_geoms = db.get_encrypted_geoms_batch(list(verify_ids))
            for vid, enc in enc_geoms.items():
                geom = crypto.decrypt_to_geometry(enc)
                if prepared_input.intersects(geom):
                    confirmed_ids.add(vid)

        result_names = []
        if confirmed_ids:
            rows = db.get_villages_by_ids(list(confirmed_ids))
            result_names = sorted([r[1] for r in rows.values()])

        print(f"    查询面=村落A → 相交村落: {result_names}")
        assert "天安村" in result_names, "应包含天安村"
        assert "建國村" in result_names, "应包含建國村（A与B部分重叠）"

        # 测试2: 查询只与C相交的区域
        query_poly_2 = Polygon([
            (116.51, 39.90), (116.53, 39.90),
            (116.53, 39.91), (116.51, 39.91),
            (116.51, 39.90),
        ])
        query_cells_2 = polygon_to_query_cells(query_poly_2, min_level=12, max_level=18, max_cells=100)
        exact_ids_2, range_conditions_2 = build_query_conditions(query_cells_2, min_level=12)
        candidate_cells_2 = db.query_cells_by_exact_and_range(exact_ids_2, range_conditions_2)

        village_hits_2 = {}
        for cell_id, village_id, is_interior, level in candidate_cells_2:
            if village_id not in village_hits_2:
                village_hits_2[village_id] = {"has_interior": False}
            if is_interior:
                village_hits_2[village_id]["has_interior"] = True

        confirmed_ids_2 = set()
        verify_ids_2 = set()
        for vid, hits in village_hits_2.items():
            if hits["has_interior"]:
                confirmed_ids_2.add(vid)
            else:
                verify_ids_2.add(vid)

        prepared_input_2 = prep(query_poly_2)
        if verify_ids_2:
            enc_geoms = db.get_encrypted_geoms_batch(list(verify_ids_2))
            for vid, enc in enc_geoms.items():
                geom = crypto.decrypt_to_geometry(enc)
                if prepared_input_2.intersects(geom):
                    confirmed_ids_2.add(vid)

        result_names_2 = []
        if confirmed_ids_2:
            rows = db.get_villages_by_ids(list(confirmed_ids_2))
            result_names_2 = sorted([r[1] for r in rows.values()])

        print(f"    查询面=C内部区域 → 相交村落: {result_names_2}")
        assert "朝阳村" in result_names_2, "应包含朝阳村"
        assert "天安村" not in result_names_2, "不应包含天安村"

        print("\n  PASS 端到端测试全部通过！")

    finally:
        db.close()
        os.unlink(key_path)
        os.unlink(db_path)


def test_query_conditions():
    """测试SQL查询条件构建"""
    print("\n" + "=" * 60)
    print("测试6: 查询条件构建")
    print("=" * 60)

    query_poly = Polygon([
        (116.39, 39.90), (116.41, 39.90),
        (116.41, 39.91), (116.39, 39.91),
        (116.39, 39.90),
    ])

    query_cells = polygon_to_query_cells(query_poly, min_level=12, max_level=18, max_cells=50)
    exact_ids, range_conditions = build_query_conditions(query_cells, min_level=12)

    print(f"  查询Cell数: {len(query_cells)}")
    print(f"  精确匹配ID数: {len(exact_ids)}")
    print(f"  范围查询条件数: {len(range_conditions)}")

    assert len(exact_ids) > 0, "应有精确匹配ID"
    # 高level Cell不需要范围查询
    print("  PASS 查询条件构建测试通过")


if __name__ == "__main__":
    test_s2_covering()
    test_ancestor_expansion()
    test_cell_range()
    test_encrypt_decrypt()
    test_query_conditions()
    test_end_to_end()

    print("\n" + "=" * 60)
    print("全部测试通过！ PASS")
    print("=" * 60)
