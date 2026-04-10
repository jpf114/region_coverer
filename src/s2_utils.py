"""
S2工具集：覆盖生成、内部/边界Cell区分、祖先展开与范围查询

核心算法说明：
- 外覆盖(get_covering)：包含村落全部区域，边界Cell可能略超出
- 内覆盖(get_interior_covering)：完全在村落内部的Cell
- 两者之差即为边界Cell（需要精确几何验证）

s2sphere API说明：
- s2sphere没有Polygon/Loop类，Region接口实现有：LatLngRect, Cap, CellUnion
- 对于多边形覆盖，采用bounding box (LatLngRect)近似 + 逐cell S2Cell.contains判断is_interior
- 这保证了：外覆盖完整（LatLngRect包含polygon），interior判断精确（逐cell验证）
"""
from dataclasses import dataclass, field

import s2sphere as s2
from shapely.geometry import Point
from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon as ShapelyMultiPolygon
from shapely.prepared import prep


@dataclass
class CellEntry:
    """单个S2 Cell的索引条目"""
    cell_id: int
    level: int
    is_interior: bool


@dataclass
class S2CoveringResult:
    """单个村落的S2覆盖结果"""
    cells: list[CellEntry] = field(default_factory=list)

    @property
    def interior_cells(self) -> list[CellEntry]:
        return [c for c in self.cells if c.is_interior]

    @property
    def boundary_cells(self) -> list[CellEntry]:
        return [c for c in self.cells if not c.is_interior]

    @property
    def cell_ids(self) -> list[int]:
        return [c.cell_id for c in self.cells]


def _validate_level(level: int, name: str) -> None:
    """验证S2 level参数"""
    if not (0 <= level <= 30):
        raise ValueError(f"{name} must be in [0, 30], got {level}")


def _shapely_to_s2_latlng_rect(polygon: ShapelyPolygon) -> s2.LatLngRect:
    """将Shapely Polygon转换为s2sphere LatLngRect（bounding box）"""
    bounds = polygon.bounds
    min_lng, min_lat, max_lng, max_lat = bounds

    latlng_rect = s2.LatLngRect(
        s2.LatLng.from_degrees(min_lat, min_lng),
        s2.LatLng.from_degrees(max_lat, max_lng),
    )
    return latlng_rect


def _check_cell_vertices_inside(cell_id_obj: s2.CellId, prepared_polygon) -> bool:
    """检查S2 Cell的4个顶点是否都在多边形内部"""
    cell = s2.Cell(cell_id_obj)
    for i in range(4):
        vertex = cell.get_vertex(i)
        vertex_latlng = s2.LatLng.from_point(vertex)
        lat = vertex_latlng.lat().degrees
        lng = vertex_latlng.lng().degrees
        if not prepared_polygon.contains(Point(lng, lat)):
            return False
    return True


def _classify_covering_cells(
    covering_cells: list[s2.CellId],
    polygon: ShapelyPolygon,
) -> list[CellEntry]:
    """
    对覆盖Cell列表进行内部/边界分类（优化版）。

    优化策略：
    1. 先检查Cell中心点是否在polygon内部（快速排除）
    2. 仅当中心点在内部时，才检查4个顶点（精确判定）
    3. 4个顶点全在内部 → interior Cell
    4. 否则 → boundary Cell
    """
    prepared_poly = prep(polygon)
    results = []

    for cell_id_obj in covering_cells:
        cell_id = cell_id_obj.id()
        level = cell_id_obj.level()
        is_interior = False

        center_latlng = cell_id_obj.to_lat_lng()
        center_lat = center_latlng.lat().degrees
        center_lng = center_latlng.lng().degrees

        if prepared_poly.contains(Point(center_lng, center_lat)):
            if _check_cell_vertices_inside(cell_id_obj, prepared_poly):
                is_interior = True

        results.append(CellEntry(
            cell_id=cell_id,
            level=level,
            is_interior=is_interior,
        ))

    return results


def polygon_to_s2_covering(
    polygon: ShapelyPolygon | ShapelyMultiPolygon,
    min_level: int = 12,
    max_level: int = 18,
    max_cells: int = 500,
) -> S2CoveringResult:
    """
    为Shapely多边形生成自适应S2覆盖，区分内部Cell与边界Cell。

    参数说明：
    - min_level=12: 内部最大Cell ≈ 1.5km，适合村落内部
    - max_level=18: 边界最小Cell ≈ 20m，适合边界精确区分
    - max_cells=500: 典型村落3~10km²需要200~800个Cell
    """
    _validate_level(min_level, "min_level")
    _validate_level(max_level, "max_level")
    if min_level > max_level:
        raise ValueError(f"min_level ({min_level}) cannot be greater than max_level ({max_level})")

    if isinstance(polygon, ShapelyMultiPolygon):
        all_cells: dict[int, CellEntry] = {}
        for part in polygon.geoms:
            part_result = polygon_to_s2_covering(part, min_level, max_level, max_cells)
            for cell in part_result.cells:
                if cell.cell_id in all_cells:
                    if not cell.is_interior:
                        all_cells[cell.cell_id].is_interior = False
                else:
                    all_cells[cell.cell_id] = cell
        return S2CoveringResult(cells=list(all_cells.values()))

    latlng_rect = _shapely_to_s2_latlng_rect(polygon)

    coverer = s2.RegionCoverer()
    coverer.min_level = min_level
    coverer.max_level = max_level
    coverer.max_cells = max_cells

    covering_cells = coverer.get_covering(latlng_rect)
    cell_entries = _classify_covering_cells(covering_cells, polygon)

    return S2CoveringResult(cells=cell_entries)


def point_to_s2_cell_id(lat: float, lng: float, level: int = 18) -> int:
    """将经纬度点转换为指定level的S2 Cell ID。"""
    _validate_level(level, "level")
    latlng = s2.LatLng.from_degrees(lat, lng)
    cell_id = s2.CellId.from_lat_lng(latlng)
    if level < 30:
        cell_id = cell_id.parent(level)
    return cell_id.id()


def expand_cell_ancestors(cell_id: int, min_level: int = 12) -> list[int]:
    """展开一个S2 Cell的所有祖先Cell ID（从当前level到min_level）。"""
    _validate_level(min_level, "min_level")
    s2_cell = s2.CellId(cell_id)
    ancestors = []

    current = s2_cell
    while current.level() >= min_level:
        ancestors.append(current.id())
        if current.level() == min_level:
            break
        current = current.parent()

    return ancestors


def get_cell_range(cell_id: int) -> tuple[int, int]:
    """获取S2 Cell的ID范围[range_min, range_max]。"""
    s2_cell = s2.CellId(cell_id)
    range_min = s2_cell.range_min().id()
    range_max = s2_cell.range_max().id()
    return range_min, range_max


def polygon_to_query_cells(
    polygon: ShapelyPolygon | ShapelyMultiPolygon,
    min_level: int = 12,
    max_level: int = 18,
    max_cells: int = 100,
) -> list[CellEntry]:
    """为查询输入的面矢量生成S2覆盖。"""
    result = polygon_to_s2_covering(polygon, min_level, max_level, max_cells)
    return result.cells


def build_query_conditions(
    query_cells: list[CellEntry],
    min_level: int = 12,
    max_db_level: int = 18,
) -> tuple[list[int], list[tuple[int, int]]]:
    """
    根据查询Cell列表构建SQL查询条件。

    参数：
    - query_cells: 查询Cell列表
    - min_level: 最小S2 level
    - max_db_level: 数据库中存储的最大level（默认18）
    """
    _validate_level(min_level, "min_level")
    _validate_level(max_db_level, "max_db_level")

    exact_ids: set[int] = set()
    range_conditions: list[tuple[int, int]] = []

    for cell_entry in query_cells:
        ancestors = expand_cell_ancestors(cell_entry.cell_id, min_level)
        exact_ids.update(ancestors)

        if cell_entry.level < max_db_level:
            range_min, range_max = get_cell_range(cell_entry.cell_id)
            range_conditions.append((range_min, range_max))

    return list(exact_ids), range_conditions
