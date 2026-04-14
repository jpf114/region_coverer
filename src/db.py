"""
数据库封装：连接池管理、批量写入、查询辅助
"""
import io
import logging
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool, sql

from .config import DBConfig

logger = logging.getLogger(__name__)


class Database:
    """数据库操作封装（支持连接池）"""

    def __init__(self, config: DBConfig):
        self._config = config
        self._pool: pool.ThreadedConnectionPool | None = None
        self._conn = None

    def connect(self) -> None:
        """建立数据库连接池"""
        if self._pool is None:
            self._pool = pool.ThreadedConnectionPool(
                minconn=self._config.pool_min,
                maxconn=self._config.pool_max,
                **self._config.get_connection_kwargs(),
            )
        self._conn = self._pool.getconn()
        self._conn.autocommit = False

    def close(self) -> None:
        """关闭数据库连接"""
        if self._conn is not None:
            if not self._conn.closed:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
            if self._pool is not None:
                self._pool.putconn(self._conn)
            self._conn = None
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None

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

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    @staticmethod
    def execute_ddl(config: DBConfig, ddl_sql: str) -> None:
        """执行DDL语句（使用独立连接和autocommit）"""
        conn = psycopg2.connect(**config.get_connection_kwargs())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(ddl_sql)
        finally:
            conn.close()

    @staticmethod
    def drop_database_safely(config: DBConfig, db_name: str) -> None:
        """安全删除数据库（使用参数化标识符）"""
        admin_config = DBConfig(
            host=config.host,
            port=config.port,
            dbname="postgres",
            user=config.user,
            password=config.password,
        )
        conn = psycopg2.connect(**admin_config.get_connection_kwargs())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                    (db_name,),
                )
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name))
                )
        finally:
            conn.close()

    @staticmethod
    def create_database_safely(config: DBConfig, db_name: str) -> None:
        """安全创建数据库（使用参数化标识符）"""
        admin_config = DBConfig(
            host=config.host,
            port=config.port,
            dbname="postgres",
            user=config.user,
            password=config.password,
        )
        conn = psycopg2.connect(**admin_config.get_connection_kwargs())
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
                )
        finally:
            conn.close()

    def insert_village(
        self,
        village_name: str,
        encrypted_geom: bytes,
        province: str | None = None,
        city: str | None = None,
        county: str | None = None,
        cell_count: int = 0,
    ) -> int:
        """插入村落主表记录，返回自增ID。"""
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
        """批量插入S2 Cell索引记录（使用COPY加速）。"""
        if not cell_records:
            return

        buf = io.StringIO()
        for cell_id, village_id, is_interior, level in cell_records:
            buf.write(f"{cell_id}\t{village_id}\t{is_interior}\t{level}\n")
        buf.seek(0)

        with self.cursor() as cur:
            cur.copy_from(
                buf,
                "village_s2_cells",
                columns=("cell_id", "village_id", "is_interior", "level"),
            )
        self.commit()

    def insert_village_with_cells(
        self,
        village_name: str,
        encrypted_geom: bytes,
        cell_records: list[tuple],
        province: str | None = None,
        city: str | None = None,
        county: str | None = None,
    ) -> int:
        """事务性插入：同时写入村落主表和Cell索引表。"""
        with self.transaction():
            with self.cursor() as cur:
                cur.execute(
                    """INSERT INTO villages (village_name, province, city, county, encrypted_geom, cell_count)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (village_name, province, city, county, encrypted_geom, len(cell_records)),
                )
                village_id = cur.fetchone()[0]

                if cell_records:
                    buf = io.StringIO()
                    for cell_id, is_interior, level in cell_records:
                        buf.write(f"{cell_id}\t{village_id}\t{is_interior}\t{level}\n")
                    buf.seek(0)
                    cur.copy_from(
                        buf,
                        "village_s2_cells",
                        columns=("cell_id", "village_id", "is_interior", "level"),
                    )

        return village_id

    def query_cells_by_ids(self, cell_ids: list[int]) -> list[tuple]:
        """根据Cell ID列表精确查询。返回: [(cell_id, village_id, is_interior, level), ...]"""
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
        """根据 Cell ID 范围条件查询（使用 UNNEST 优化）。"""
        if not range_conditions:
            return []

        # 使用 UNNEST 方式替代大量 OR 条件，提升 SQL 效率
        with self.cursor() as cur:
            cur.execute(
                """SELECT cell_id, village_id, is_interior, level
                   FROM village_s2_cells
                   WHERE EXISTS (
                       SELECT 1 FROM UNNEST(%s::bigint[], %s::bigint[]) AS ranges(rmin, rmax)
                       WHERE cell_id BETWEEN rmin AND rmax
                   )""",
                ([r[0] for r in range_conditions], [r[1] for r in range_conditions]),
            )
            return cur.fetchall()

    def query_cells_by_exact_and_range(
        self,
        exact_ids: list[int],
        range_conditions: list[tuple[int, int]],
    ) -> list[tuple]:
        """组合查询：精确匹配 + 范围查询（使用 UNNEST 优化）。"""
        results = {}

        # 使用 UNNEST 一次性查询精确匹配和范围匹配
        exact_array = exact_ids if exact_ids else []
        range_mins = [r[0] for r in range_conditions] if range_conditions else []
        range_maxs = [r[1] for r in range_conditions] if range_conditions else []

        with self.cursor() as cur:
            cur.execute(
                """SELECT cell_id, village_id, is_interior, level
                   FROM village_s2_cells
                   WHERE (
                       cell_id = ANY(%s::bigint[])
                       OR EXISTS (
                           SELECT 1 FROM UNNEST(%s::bigint[], %s::bigint[]) AS ranges(rmin, rmax)
                           WHERE cell_id BETWEEN rmin AND rmax
                       )
                   )""",
                (exact_array, range_mins, range_maxs),
            )
            for row in cur.fetchall():
                results[row[0]] = row

        return list(results.values())

    def query_cells_with_village_info(
        self, cell_ids: list[int]
    ) -> list[tuple]:
        """一次性查询Cell信息+村落信息（减少往返）。返回: [(cell_id, village_id, is_interior, level, village_name, province, city, county), ...]"""
        if not cell_ids:
            return []

        with self.cursor() as cur:
            cur.execute(
                """SELECT c.cell_id, c.village_id, c.is_interior, c.level,
                          v.village_name, v.province, v.city, v.county
                   FROM village_s2_cells c
                   JOIN villages v ON c.village_id = v.id
                   WHERE c.cell_id = ANY(%s)""",
                (cell_ids,),
            )
            return cur.fetchall()

    def get_village_by_id(self, village_id: int) -> tuple | None:
        """根据ID获取村落记录（不含加密几何）。"""
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
        """批量获取村落信息（不含加密几何）。"""
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
        """批量获取村落的加密几何数据。"""
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
