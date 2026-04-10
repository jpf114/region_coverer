#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
完整测试：PostgreSQL + 中国市级矢量数据 (china.geojson)
数据库: region_coverer (独立库，不污染默认postgres)
"""
import os
import sys
import time
import logging
import tempfile

os.environ['PYTHONIOENCODING'] = 'utf-8'
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)

from cryptography.fernet import Fernet
from src.config import AppConfig, S2Config, DBConfig, CryptoConfig, QueryConfig
from src.crypto import GeometryCrypto
from src.db import Database
from src.indexing import read_geojson_features, extract_village_info, process_single_village
from src.query import locate_village_by_point, locate_villages_by_polygon
from shapely.geometry import shape, Polygon, MultiPolygon

PG_HOST = os.getenv("DB_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("DB_PORT", "5432"))
PG_USER = os.getenv("DB_USER", "postgres")
PG_PASS = os.getenv("DB_PASSWORD", "")
NEW_DB = os.getenv("DB_NAME", "region_coverer")

KEY_PATH = os.path.join(_project_root, "test_key.bin")
if not os.path.exists(KEY_PATH):
    with open(KEY_PATH, 'wb') as f:
        f.write(Fernet.generate_key())
    log.info("Generated Fernet key")

app_config = AppConfig(
    s2=S2Config(min_level=12, max_level=18, max_cells=500),
    db=DBConfig(host=PG_HOST, port=PG_PORT, dbname=NEW_DB, user=PG_USER, password=PG_PASS),
    crypto=CryptoConfig(key_path=KEY_PATH),
    query=QueryConfig(max_cells=100, max_db_level=18),
)
GEOJSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'china.geojson')


def main():
    t_start = time.time()

    log.info("=" * 60)
    log.info("STEP 0: Recreate database '%s'", NEW_DB)
    Database.drop_database_safely(app_config.db, NEW_DB)
    Database.create_database_safely(app_config.db, NEW_DB)
    log.info("  Database '%s' created", NEW_DB)

    log.info("=" * 60)
    log.info("STEP 1: Create schema")
    schema_sql = """
        CREATE TABLE villages (
            id BIGSERIAL PRIMARY KEY, village_name VARCHAR(100) NOT NULL,
            province VARCHAR(50), city VARCHAR(50), county VARCHAR(50),
            encrypted_geom BYTEA NOT NULL, cell_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX idx_villages_county ON villages (province, city, county);
        CREATE TABLE village_s2_cells (
            cell_id BIGINT NOT NULL, village_id BIGINT NOT NULL REFERENCES villages(id) ON DELETE CASCADE,
            is_interior BOOLEAN NOT NULL, level SMALLINT NOT NULL, PRIMARY KEY (cell_id, village_id)
        );
        CREATE INDEX idx_vsc_cell ON village_s2_cells (cell_id);
        CREATE INDEX idx_vsc_village ON village_s2_cells (village_id);
        CREATE INDEX idx_vsc_level ON village_s2_cells (level);
    """
    Database.execute_ddl(app_config.db, schema_sql)
    log.info("  Schema OK")

    log.info("=" * 60)
    log.info("STEP 2: Index china.geojson (477 features)")
    db = Database(app_config.db)
    crypto = GeometryCrypto(app_config.crypto)
    db.connect()

    idx_start = time.time()
    count, errors = 0, 0
    for feature in read_geojson_features(GEOJSON):
        geom = shape(feature.get("geometry", {}))
        if not isinstance(geom, (Polygon, MultiPolygon)):
            continue
        vi = extract_village_info(feature)
        vi['village_name'] = feature.get("properties", {}).get('name', vi['village_name'])
        try:
            process_single_village(geom, vi, db, crypto, app_config)
        except Exception as e:
            errors += 1
            log.error("  FAIL [%s]: %s", vi.get('village_name'), e)
        count += 1
        if count % 50 == 0:
            log.info("  Progress: %d features, %.1fs elapsed", count, time.time() - idx_start)

    idx_elapsed = time.time() - idx_start
    log.info("  Indexed %d features in %.1fs (%d errors)", count, idx_elapsed, errors)

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM villages"); vc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM village_s2_cells"); cc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM village_s2_cells WHERE is_interior"); ic = cur.fetchone()[0]
    log.info("  DB: %d villages, %d cells (%d interior, %d boundary)", vc, cc, ic, cc - ic)

    log.info("=" * 60)
    log.info("STEP 3: Point queries")
    test_pts = [
        (116.418757, 39.917544, "东城区"),
        (116.37, 39.94, "西城区"),
        (116.46, 39.92, "朝阳区"),
        (121.47, 31.23, "黄浦区"),
        (113.26, 23.13, "越秀区"),
        (114.06, 22.55, "福田区"),
        (104.07, 30.67, "武侯区"),
        (106.55, 29.56, "渝中区"),
    ]
    hit, total_ms = 0, 0.0
    for lng, lat, exp in test_pts:
        t0 = time.time()
        r = locate_village_by_point(lng, lat, db, crypto, app_config)
        ms = (time.time() - t0) * 1000
        total_ms += ms
        if r:
            ok = "PASS" if exp in r.village_name else "MISMATCH"
            log.info("  (%.3f,%.3f)->%s [%s] %.1fms", lng, lat, r.village_name, ok, ms)
            hit += (1 if exp in r.village_name else 0)
        else:
            log.info("  (%.3f,%.3f)->NOT FOUND (exp:%s) %.1fms", lng, lat, exp, ms)
    log.info("  Result: %d/%d hits, avg %.1fms", hit, len(test_pts), total_ms / len(test_pts))

    log.info("=" * 60)
    log.info("STEP 4: Polygon queries")
    tests = [
        ("Beijing Core", Polygon([
            (116.35, 39.88), (116.43, 39.88), (116.43, 39.95), (116.35, 39.95)]), 2),
        ("Beijing Greater", Polygon([
            (116.20, 39.70), (116.55, 39.70), (116.55, 40.10), (116.20, 40.10)]), 5),
        ("Shanghai Central", Polygon([
            (121.40, 31.18), (121.52, 31.18), (121.52, 31.28), (121.40, 31.28)]), 2),
        ("Guangzhou-Shenzhen", Polygon([
            (113.20, 22.50), (114.10, 22.50), (114.10, 23.20), (113.20, 23.20)]), 3),
    ]
    total_ms2 = 0.0
    for name, poly, exp_min in tests:
        t0 = time.time()
        results = locate_villages_by_polygon(poly, db, crypto, app_config)
        ms = (time.time() - t0) * 1000
        total_ms2 += ms
        names = [r.village_name for r in results]
        ok = "PASS" if len(results) >= exp_min else "FEW"
        log.info("  [%s] %d villages [%s] %.1fms: %s",
                 name, len(results), ok, ms,
                 ", ".join(names[:8]) + ("..." if len(names) > 8 else ""))
    log.info("  Avg %.1fms/query", total_ms2 / len(tests))

    log.info("=" * 60)
    log.info("STEP 5: Statistics")
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM villages"); vc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM village_s2_cells"); cc = cur.fetchone()[0]
        cur.execute("SELECT level, COUNT(*), SUM(CASE WHEN is_interior THEN 1 ELSE 0 END) FROM village_s2_cells GROUP BY level ORDER BY level")
        lvls = cur.fetchall()
        cur.execute("SELECT AVG(cell_count), MIN(cell_count), MAX(cell_count) FROM villages")
        avg_c, min_c, max_c = cur.fetchone()
        cur.execute("SELECT SUM(pg_column_size(encrypted_geom)) FROM villages")
        enc_sz = cur.fetchone()[0] or 0
        cur.execute("SELECT pg_relation_size('villages')")
        vil_sz = cur.fetchone()[0]
        cur.execute("SELECT pg_relation_size('village_s2_cells')")
        cell_sz = cur.fetchone()[0]

    log.info("  Villages: %d", vc)
    log.info("  Total cells: %d", cc)
    log.info("  Avg cells/village: %.1f (min=%d, max=%d)", float(avg_c or 0), min_c or 0, max_c or 0)
    log.info("  Encrypted geom: %.1f KB", enc_sz / 1024)
    log.info("  Table sizes: villages=%.1fKB, cells=%.1fKB", vil_sz / 1024, cell_sz / 1024)
    log.info("  Level distribution:")
    for lvl, cnt, ic in lvls:
        log.info("    L%d: %d (%d interior, %d boundary)", lvl, cnt, ic, cnt - ic)

    db.close()

    log.info("=" * 60)
    log.info("ALL DONE in %.1fs", time.time() - t_start)
    log.info("=" * 60)


if __name__ == '__main__':
    main()
