"""
数据库封装：连接管理、批量写入、查询辅助
"""
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Generator

from .config import DBConfig


class Database:
    """数据库操作封装"""

    def __init__(self, config: DBConfig):
        self._config = config
        self._conn = None

    def connect(self) -> None:
        """建立数据库连接"""
        self._conn = psycopg2.connect(self._config.dsn)
        self._conn.autocommit = True  # autocommit避免DDL死锁

    def close(self) -> None:
        """关闭数据库连接"""
        if self._conn and not self._conn.closed:
            self._conn.close()

    @property
    def conn(self):
        """获取当前连接"""
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn

    @contextmanager
    def cursor(self, **kwargs) -> Generator:
        """获取游标的上下文管理器"""
        cur = self.conn.cursor(**kwargs)
        try:
            yield cur
        finally:
            cur.close()

    def commit(self) -> None:
        """提交当前事务"""
        self.conn.commit()

    def rollback(self) -> None:
        """回滚当前事务"""
        self.conn.rollback()

    # ---------- 写入方法 ----------

    def insert_village(
        self,
        village_name: str,
        encrypted_geom: bytes,
        province: str = None,
        city: str = None,
        county: str = None,
        cell_count: int = 0,
    ) -> int:
        """
        插入村落主表记录，返回自增ID。
        """
        with self.cursor() as cur:
            cur.execute(
                """INSERT INTO villages (village_name, province, city, county, encrypted_geom, cell_count)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (village_name, province, city, county, encrypted_geom, cell_count),
            )
            village_id = cur.fetchone()[0]
        self.commit()
        return village_id

    def batch_insert_cells(self, cell_records: list[tuple]) -> None:
        """
        批量插入S2 Cell索引记录。

        cell_records格式: [(cell_id, village_id, is_interior, level), ...]
        使用executemany批量写入，比单条INSERT快数十倍。
        """
        if not cell_records:
            return

        with self.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO village_s2_cells (cell_id, village_id, is_interior, level)
                   VALUES %s
                   ON CONFLICT (cell_id, village_id) DO NOTHING""",
                cell_records,
                template="(%s, %s, %s, %s)",
                page_size=1000,
            )
        self.commit()

    def insert_village_with_cells(
        self,
        village_name: str,
        encrypted_geom: bytes,
        cell_records: list[tuple],
        province: str = None,
        city: str = None,
        county: str = None,
    ) -> int:
        """
        事务性插入：同时写入村落主表和Cell索引表。

        cell_records格式: [(cell_id, is_interior, level), ...]
        """
        with self.cursor() as cur:
            # 1. 插入村落主表
            cur.execute(
                """INSERT INTO villages (village_name, province, city, county, encrypted_geom, cell_count)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   RETURNING id""",
                (village_name, province, city, county, encrypted_geom, len(cell_records)),
            )
            village_id = cur.fetchone()[0]

            # 2. 批量插入Cell索引
            if cell_records:
                full_records = [
                    (cell_id, village_id, is_interior, level)
                    for cell_id, is_interior, level in cell_records
                ]
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO village_s2_cells (cell_id, village_id, is_interior, level)
                       VALUES %s
                       ON CONFLICT (cell_id, village_id) DO NOTHING""",
                    full_records,
                    template="(%s, %s, %s, %s)",
                    page_size=1000,
                )

        self.commit()
        return village_id

    # ---------- 查询方法 ----------

    def query_cells_by_ids(self, cell_ids: list[int]) -> list[tuple]:
        """
        根据Cell ID列表精确查询。

        返回: [(cell_id, village_id, is_interior, level), ...]
        """
        if not cell_ids:
            return []

        with self.cursor() as cur:
            cur.execute(
                """SELECT cell_id, village_id, is_interior, level
                   FROM village_s2_cells
                   WHERE cell_id = ANY(%s)""",
                (cell_ids,),
            )
            return cur.fetchall()

    def query_cells_by_ranges(
        self, range_conditions: list[tuple[int, int]]
    ) -> list[tuple]:
        """
        根据Cell ID范围条件查询（捕获后代Cell）。

        range_conditions: [(range_min, range_max), ...]
        返回: [(cell_id, village_id, is_interior, level), ...]
        """
        if not range_conditions:
            return []

        # 构建OR条件
        conditions = []
        params = []
        for range_min, range_max in range_conditions:
            conditions.append("cell_id BETWEEN %s AND %s")
            params.extend([range_min, range_max])

        sql = (
            "SELECT cell_id, village_id, is_interior, level "
            "FROM village_s2_cells "
            "WHERE " + " OR ".join(conditions)
        )

        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def query_cells_by_exact_and_range(
        self,
        exact_ids: list[int],
        range_conditions: list[tuple[int, int]],
    ) -> list[tuple]:
        """
        组合查询：精确匹配 + 范围查询（面矢量查询的核心）。

        返回去重后的: [(cell_id, village_id, is_interior, level), ...]
        """
        results = {}

        # 1. 精确匹配
        if exact_ids:
            for row in self.query_cells_by_ids(exact_ids):
                results[row[0]] = row  # cell_id作为key去重

        # 2. 范围查询
        if range_conditions:
            for row in self.query_cells_by_ranges(range_conditions):
                results[row[0]] = row  # cell_id作为key去重

        return list(results.values())

    def get_village_by_id(self, village_id: int) -> tuple | None:
        """
        根据ID获取村落记录（不含加密几何）。

        返回: (id, village_name, province, city, county, cell_count, created_at) 或 None
        """
        with self.cursor() as cur:
            cur.execute(
                """SELECT id, village_name, province, city, county, cell_count, created_at
                   FROM villages WHERE id = %s""",
                (village_id,),
            )
            return cur.fetchone()

    def get_encrypted_geom(self, village_id: int) -> bytes | None:
        """获取村落的加密几何数据"""
        with self.cursor() as cur:
            cur.execute(
                "SELECT encrypted_geom FROM villages WHERE id = %s",
                (village_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            val = row[0]
            return bytes(val) if not isinstance(val, bytes) else val

    def get_villages_by_ids(self, village_ids: list[int]) -> dict[int, tuple]:
        """
        批量获取村落信息（不含加密几何）。

        返回: {village_id: (id, village_name, province, city, county), ...}
        """
        if not village_ids:
            return {}

        with self.cursor() as cur:
            cur.execute(
                """SELECT id, village_name, province, city, county
                   FROM villages WHERE id = ANY(%s)""",
                (village_ids,),
            )
            return {row[0]: row for row in cur.fetchall()}

    def get_encrypted_geoms_batch(self, village_ids: list[int]) -> dict[int, bytes]:
        """
        批量获取村落的加密几何数据。

        返回: {village_id: encrypted_geom_bytes, ...}
        """
        if not village_ids:
            return {}

        with self.cursor() as cur:
            cur.execute(
                "SELECT id, encrypted_geom FROM villages WHERE id = ANY(%s)",
                (village_ids,),
            )
            result = {}
            for row in cur.fetchall():
                val = row[1]
                result[row[0]] = bytes(val) if not isinstance(val, bytes) else val
            return result
