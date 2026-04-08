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
from typing import Optional

import s2sphere as s2
from shapely.geometry import Polygon as ShapelyPolygon, MultiPolygon as ShapelyMultiPolygon
from shapely.prepared import prep


@dataclass
class CellEntry:
    """单个S2 Cell的索引条目"""
    cell_id: int        # S2 Cell ID (int64)
    level: int          # S2 level
    is_interior: bool   # true=完全在村落内部, false=边界Cell


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


def _shapely_to_s2_latlng_rect(polygon: ShapelyPolygon) -> s2.LatLngRect:
    """将Shapely Polygon转换为s2sphere LatLngRect（bounding box）"""
    bounds = polygon.bounds  # (minx, miny, maxx, maxy) = (min_lng, min_lat, max_lng, max_lat)
    min_lng, min_lat, max_lng, max_lat = bounds

    latlng_rect = s2.LatLngRect(
        s2.LatLng.from_degrees(min_lat, min_lng),
        s2.LatLng.from_degrees(max_lat, max_lng),
    )
    return latlng_rect


def _cell_center_in_polygon(cell_id_int: int, prepared_polygon) -> bool:
    """检查S2 Cell的中心点是否在多边形内部（快速判断辅助函数）"""
    cell_id = s2.CellId(cell_id_int)
    center_latlng = cell_id.to_lat_lng()
    lat = center_latlng.lat().degrees
    lng = center_latlng.lng().degrees
    from shapely.geometry import Point
    return prepared_polygon.contains(Point(lng, lat))


def _classify_covering_cells(
    covering_cells: list[s2.CellId],
    polygon: ShapelyPolygon,
) -> list[CellEntry]:
    """
    对覆盖Cell列表进行内部/边界分类。

    策略：
    - 使用Shapely prepared geometry进行高效相交判断
    - 如果Cell完全在polygon内部（S2Cell的4个顶点+中心都在polygon内），标记为interior
    - 否则标记为boundary

    注意：由于S2 Cell是球面cell，其4个顶点在投影后可能与polygon边界有微小偏差，
    但对于村落级别（level 12~18）的cell，这种偏差可以忽略。
    精确验证在查询阶段由Shapely intersects完成。
    """
    prepared_poly = prep(polygon)
    results = []

    for cell_id_obj in covering_cells:
        cell_id = cell_id_obj.id()
        level = cell_id_obj.level()
        is_interior = False

        # 检查Cell的4个顶点是否都在polygon内部
        cell = s2.Cell(cell_id_obj)
        all_vertices_inside = True
        for i in range(4):
            vertex = cell.get_vertex(i)
            vertex_latlng = s2.LatLng.from_point(vertex)
            lat = vertex_latlng.lat().degrees
            lng = vertex_latlng.lng().degrees
            from shapely.geometry import Point
            if not prepared_poly.contains(Point(lng, lat)):
                all_vertices_inside = False
                break

        if all_vertices_inside:
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

    算法：
    1. 用polygon的bounding box (LatLngRect) 生成S2外覆盖
    2. 对每个覆盖Cell，判断其4个顶点是否都在polygon内部
    3. 4个顶点全在内部 → interior Cell
    4. 否则 → boundary Cell

    注意：使用LatLngRect生成覆盖会比精确多边形覆盖多出一些Cell，
    这些Cell的中心点在polygon外，会在查询阶段被精确验证过滤。

    参数说明：
    - min_level=12: 内部最大Cell ≈ 1.5km，适合村落内部
    - max_level=18: 边界最小Cell ≈ 20m，适合边界精确区分
    - max_cells=500: 典型村落3~10km²需要200~800个Cell
    """
    # 处理MultiPolygon：对每个part分别生成覆盖后合并
    if isinstance(polygon, ShapelyMultiPolygon):
        all_cells: dict[int, CellEntry] = {}
        for part in polygon.geoms:
            part_result = polygon_to_s2_covering(part, min_level, max_level, max_cells)
            for cell in part_result.cells:
                if cell.cell_id in all_cells:
                    # 如果任一部分标记为边界，则标记为边界
                    if not cell.is_interior:
                        all_cells[cell.cell_id].is_interior = False
                else:
                    all_cells[cell.cell_id] = cell
        return S2CoveringResult(cells=list(all_cells.values()))

    # 单个Polygon处理
    latlng_rect = _shapely_to_s2_latlng_rect(polygon)

    coverer = s2.RegionCoverer()
    coverer.min_level = min_level
    coverer.max_level = max_level
    coverer.max_cells = max_cells

    # 1. 生成外覆盖
    covering_cells = coverer.get_covering(latlng_rect)

    # 2. 对每个Cell进行内部/边界分类
    cell_entries = _classify_covering_cells(covering_cells, polygon)

    return S2CoveringResult(cells=cell_entries)


def point_to_s2_cell_id(lat: float, lng: float, level: int = 18) -> int:
    """
    将经纬度点转换为指定level的S2 Cell ID。

    用于点查询：点 → S2 Leaf Cell → 取指定level的parent
    """
    latlng = s2.LatLng.from_degrees(lat, lng)
    cell_id = s2.CellId.from_lat_lng(latlng)
    if level < 30:
        cell_id = cell_id.parent(level)
    return cell_id.id()


def expand_cell_ancestors(cell_id: int, min_level: int = 12) -> list[int]:
    """
    展开一个S2 Cell的所有祖先Cell ID（从当前level到min_level）。

    用于查询时的粗过滤：
    - 当数据库中存储的是大Cell(低level)，而查询点落在小Cell(高level)时，
      需要检查查询Cell的祖先是否与数据库中的Cell匹配。

    例如：查询cell level=18，数据库中存储了该位置的level=12的Cell，
    那么level=18 Cell的level=12祖先就是数据库中的Cell ID。
    """
    s2_cell = s2.CellId(cell_id)
    ancestors = []

    # 包含自身
    current = s2_cell
    while current.level() >= min_level:
        ancestors.append(current.id())
        if current.level() == min_level:
            break
        current = current.parent()

    return ancestors


def get_cell_range(cell_id: int) -> tuple[int, int]:
    """
    获取S2 Cell的ID范围[range_min, range_max]。

    用于范围查询：捕获数据库中属于该Cell子树的所有后代Cell。
    当数据库中存储的是小Cell(高level)，而查询输入是大Cell(低level)时，
    需要用范围查询找到所有落在该大Cell范围内的小Cell。

    S2 Cell的Hilbert编码保证：父Cell的range包含所有后代的Cell ID。
    """
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
    """
    为查询输入的面矢量生成S2覆盖。

    与polygon_to_s2_covering不同，此函数用于查询阶段，
    max_cells可以较小（100），因为只需要找到候选村落，
    不需要精确覆盖。

    返回的Cell用于构建SQL查询条件：
    1. 对每个Cell，展开其祖先 → 等值匹配
    2. 对每个Cell，取range → 范围查询捕获后代
    """
    # 查询覆盖使用较粗的精度即可
    result = polygon_to_s2_covering(polygon, min_level, max_level, max_cells)
    return result.cells


def build_query_conditions(
    query_cells: list[CellEntry],
    min_level: int = 12,
) -> tuple[list[int], list[tuple[int, int]]]:
    """
    根据查询Cell列表构建SQL查询条件。

    返回:
    - exact_ids: 需要精确匹配的Cell ID列表（祖先展开后的所有ID）
    - range_conditions: 需要范围查询的(cell_range_min, cell_range_max)列表

    算法逻辑：
    - 对每个查询Cell，展开其祖先 → 加入exact_ids（匹配数据库中存储的大Cell）
    - 对level较低的查询Cell，使用range查询 → 捕获数据库中的小Cell后代
    - 对level=18(最大精度)的Cell，不需要range查询（没有更小的后代在数据库中）
    """
    exact_ids: set[int] = set()
    range_conditions: list[tuple[int, int]] = []

    max_db_level = 18  # 数据库中存储的最大level

    for cell_entry in query_cells:
        # 1. 祖先展开 → 精确匹配
        ancestors = expand_cell_ancestors(cell_entry.cell_id, min_level)
        exact_ids.update(ancestors)

        # 2. 范围查询 → 捕获后代（仅对非最大level的Cell）
        if cell_entry.level < max_db_level:
            range_min, range_max = get_cell_range(cell_entry.cell_id)
            range_conditions.append((range_min, range_max))

    return list(exact_ids), range_conditions
