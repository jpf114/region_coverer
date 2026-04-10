"""
村落矢量入库管道：GeoJSON/Shapefile读取 → S2覆盖生成 → 加密geometry → 批量写入
"""
import json
import logging
from pathlib import Path
from typing import Iterator

from shapely.geometry import shape, Polygon, MultiPolygon

from .config import AppConfig, config as default_config
from .crypto import GeometryCrypto
from .db import Database
from .s2_utils import polygon_to_s2_covering

logger = logging.getLogger(__name__)


def read_geojson_features(geojson_path: str | Path) -> Iterator[dict]:
    """从GeoJSON文件读取Feature迭代器。"""
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if data.get("type") == "FeatureCollection":
        for feature in data.get("features", []):
            yield feature
    elif data.get("type") == "Feature":
        yield data
    else:
        yield {"type": "Feature", "geometry": data, "properties": {}}


def extract_village_info(feature: dict) -> dict:
    """从GeoJSON Feature中提取村落信息。"""
    props = feature.get("properties", {})

    return {
        "village_name": (
            props.get("village_name")
            or props.get("name")
            or props.get("NAME")
            or props.get("VILLAGE_NAME")
            or ""
        ),
        "province": props.get("province", props.get("PROVINCE", "")),
        "city": props.get("city", props.get("CITY", "")),
        "county": props.get("county", props.get("COUNTY", props.get("district", ""))),
    }


def process_single_village(
    geom: Polygon | MultiPolygon,
    village_info: dict,
    db: Database,
    crypto: GeometryCrypto,
    app_config: AppConfig | None = None,
) -> int:
    """处理单个村落：生成S2覆盖 → 加密geometry → 写入数据库。"""
    if app_config is None:
        app_config = default_config

    covering = polygon_to_s2_covering(
        geom,
        min_level=app_config.s2.min_level,
        max_level=app_config.s2.max_level,
        max_cells=app_config.s2.max_cells,
    )

    logger.info(
        "村落[%s] S2覆盖: %d个Cell (内部%d, 边界%d)",
        village_info.get("village_name", "?"),
        len(covering.cells),
        len(covering.interior_cells),
        len(covering.boundary_cells),
    )

    encrypted_geom = crypto.encrypt_geometry(geom)

    cell_records = [
        (cell.cell_id, cell.is_interior, cell.level)
        for cell in covering.cells
    ]

    village_id = db.insert_village_with_cells(
        village_name=village_info["village_name"],
        encrypted_geom=encrypted_geom,
        cell_records=cell_records,
        province=village_info.get("province"),
        city=village_info.get("city"),
        county=village_info.get("county"),
    )

    logger.info("村落[%s]入库完成, ID=%d", village_info.get("village_name", "?"), village_id)
    return village_id


def index_geojson_file(
    geojson_path: str | Path,
    app_config: AppConfig | None = None,
) -> list[int]:
    """将GeoJSON文件中的所有村落入库。"""
    if app_config is None:
        app_config = default_config

    db = Database(app_config.db)
    crypto = GeometryCrypto(app_config.crypto)
    village_ids = []

    try:
        db.connect()
        for feature in read_geojson_features(geojson_path):
            geom = shape(feature.get("geometry", {}))

            if not isinstance(geom, (Polygon, MultiPolygon)):
                logger.warning("跳过非面类型Geometry: %s", geom.geom_type)
                continue

            village_info = extract_village_info(feature)
            village_id = process_single_village(geom, village_info, db, crypto, app_config)
            village_ids.append(village_id)

    finally:
        db.close()

    logger.info("入库完成，共%d个村落", len(village_ids))
    return village_ids


def batch_index_geojson_files(
    geojson_paths: list[str | Path],
    app_config: AppConfig | None = None,
) -> list[int]:
    """批量入库多个GeoJSON文件。"""
    if app_config is None:
        app_config = default_config

    db = Database(app_config.db)
    crypto = GeometryCrypto(app_config.crypto)
    all_village_ids = []

    try:
        db.connect()
        for path in geojson_paths:
            logger.info("开始处理文件: %s", path)
            for feature in read_geojson_features(path):
                geom = shape(feature.get("geometry", {}))

                if not isinstance(geom, (Polygon, MultiPolygon)):
                    logger.warning("跳过非面类型Geometry: %s", geom.geom_type)
                    continue

                village_info = extract_village_info(feature)
                village_id = process_single_village(geom, village_info, db, crypto, app_config)
                all_village_ids.append(village_id)

    finally:
        db.close()

    logger.info("批量入库完成，共%d个村落", len(all_village_ids))
    return all_village_ids
