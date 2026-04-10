"""
查询服务：面矢量查询 + 点查询

面矢量查询（核心新增）：输入Polygon → 返回所有相交的村落名称列表
点查询（修复）：输入经纬度 → 逐级向上回溯 → 返回所属村落
"""
import logging
from dataclasses import dataclass

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon as ShapelyMultiPolygon
from shapely.prepared import prep

from .config import AppConfig, config as default_config
from .crypto import GeometryCrypto
from .db import Database
from .s2_utils import (
    point_to_s2_cell_id,
    expand_cell_ancestors,
    build_query_conditions,
    polygon_to_query_cells,
)

logger = logging.getLogger(__name__)


@dataclass
class VillageResult:
    """村落查询结果"""
    village_id: int
    village_name: str
    province: str
    city: str
    county: str

    def __repr__(self):
        return f"VillageResult(id={self.village_id}, name={self.village_name}, {self.province}/{self.city}/{self.county})"


def _village_row_to_result(row: tuple) -> VillageResult:
    """将数据库行(id, village_name, province, city, county)转为VillageResult"""
    return VillageResult(
        village_id=row[0],
        village_name=row[1],
        province=row[2] or "",
        city=row[3] or "",
        county=row[4] or "",
    )


def _validate_coordinates(lng: float, lat: float) -> None:
    """验证经纬度范围"""
    if not (-180 <= lng <= 180):
        raise ValueError(f"经度必须在[-180, 180]范围内，当前值: {lng}")
    if not (-90 <= lat <= 90):
        raise ValueError(f"纬度必须在[-90, 90]范围内，当前值: {lat}")


def locate_village_by_point(
    lng: float,
    lat: float,
    db: Database,
    crypto: GeometryCrypto,
    app_config: AppConfig | None = None,
) -> VillageResult | None:
    """根据经纬度快速定位所属村落。"""
    _validate_coordinates(lng, lat)

    if app_config is None:
        app_config = default_config

    min_level = app_config.s2.min_level
    max_level = app_config.s2.max_level

    leaf_cell_id = point_to_s2_cell_id(lat, lng, level=max_level)
    ancestor_ids = expand_cell_ancestors(leaf_cell_id, min_level)

    cell_rows_with_village = db.query_cells_with_village_info(ancestor_ids)

    if not cell_rows_with_village:
        return None

    cell_rows_with_village.sort(key=lambda r: r[3], reverse=True)

    boundary_candidates: set[int] = set()
    village_info_cache: dict[int, tuple] = {}

    for row in cell_rows_with_village:
        cell_id, village_id, is_interior, level, village_name, province, city, county = row
        village_info_cache[village_id] = (village_id, village_name, province, city, county)

        if is_interior:
            return _village_row_to_result(village_info_cache[village_id])
        else:
            boundary_candidates.add(village_id)

    if boundary_candidates:
        query_point = ShapelyPoint(lng, lat)
        encrypted_geoms = db.get_encrypted_geoms_batch(list(boundary_candidates))

        for village_id, encrypted_geom in encrypted_geoms.items():
            try:
                polygon = crypto.decrypt_to_geometry(encrypted_geom)
                if polygon.contains(query_point):
                    if village_id in village_info_cache:
                        return _village_row_to_result(village_info_cache[village_id])
            except Exception as e:
                logger.error("解密验证village_id=%d失败: %s", village_id, e)

    return None


def locate_villages_by_polygon(
    polygon: ShapelyPolygon | ShapelyMultiPolygon,
    db: Database,
    crypto: GeometryCrypto,
    app_config: AppConfig | None = None,
) -> list[VillageResult]:
    """面矢量查询：返回与输入Polygon相交的所有村落。"""
    if app_config is None:
        app_config = default_config

    min_level = app_config.s2.min_level
    max_level = app_config.s2.max_level
    query_max_cells = app_config.query.max_cells
    max_db_level = app_config.query.max_db_level

    query_cells = polygon_to_query_cells(
        polygon,
        min_level=min_level,
        max_level=max_level,
        max_cells=query_max_cells,
    )

    exact_ids, range_conditions = build_query_conditions(
        query_cells, min_level, max_db_level
    )

    candidate_cells = db.query_cells_by_exact_and_range(exact_ids, range_conditions)

    if not candidate_cells:
        return []

    village_hits: dict[int, dict] = {}

    for cell_id, village_id, is_interior, level in candidate_cells:
        if village_id not in village_hits:
            village_hits[village_id] = {"has_interior": False, "cell_count": 0}
        village_hits[village_id]["cell_count"] += 1
        if is_interior:
            village_hits[village_id]["has_interior"] = True

    confirmed_ids: set[int] = set()
    verify_ids: set[int] = set()

    for village_id, hits in village_hits.items():
        if hits["has_interior"]:
            confirmed_ids.add(village_id)
        else:
            verify_ids.add(village_id)

    if verify_ids:
        prepared_input = prep(polygon)
        encrypted_geoms = db.get_encrypted_geoms_batch(list(verify_ids))

        for village_id, encrypted_geom in encrypted_geoms.items():
            try:
                village_geom = crypto.decrypt_to_geometry(encrypted_geom)
                if prepared_input.intersects(village_geom):
                    confirmed_ids.add(village_id)
            except Exception as e:
                logger.error("解密验证village_id=%d失败: %s", village_id, e)

    if not confirmed_ids:
        return []

    village_rows = db.get_villages_by_ids(list(confirmed_ids))
    results = [
        _village_row_to_result(row)
        for row in village_rows.values()
    ]

    results.sort(key=lambda r: r.village_name)

    logger.info(
        "面查询完成: 候选%d个村落, 确认%d个相交",
        len(village_hits), len(results),
    )

    return results


def query_villages_by_polygon(
    polygon: ShapelyPolygon | ShapelyMultiPolygon,
    app_config: AppConfig | None = None,
) -> list[VillageResult]:
    """便捷函数：面矢量查询，自动管理数据库连接。"""
    if app_config is None:
        app_config = default_config

    db = Database(app_config.db)
    crypto = GeometryCrypto(app_config.crypto)

    try:
        db.connect()
        return locate_villages_by_polygon(polygon, db, crypto, app_config)
    finally:
        db.close()


def query_village_by_point(
    lng: float,
    lat: float,
    app_config: AppConfig | None = None,
) -> VillageResult | None:
    """便捷函数：点查询，自动管理数据库连接。"""
    if app_config is None:
        app_config = default_config

    db = Database(app_config.db)
    crypto = GeometryCrypto(app_config.crypto)

    try:
        db.connect()
        return locate_village_by_point(lng, lat, db, crypto, app_config)
    finally:
        db.close()
