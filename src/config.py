"""
全局配置：S2参数、数据库连接、加密密钥路径
"""
import os
from dataclasses import dataclass, field


@dataclass
class S2Config:
    """S2 Region Coverer 参数"""
    min_level: int = 10
    max_level: int = 18
    max_cells: int = 2000

    def __post_init__(self):
        if not (0 <= self.min_level <= 30):
            raise ValueError(f"min_level must be in [0, 30], got {self.min_level}")
        if not (0 <= self.max_level <= 30):
            raise ValueError(f"max_level must be in [0, 30], got {self.max_level}")
        if self.min_level > self.max_level:
            raise ValueError(f"min_level ({self.min_level}) cannot be greater than max_level ({self.max_level})")
        if self.max_cells < 1:
            raise ValueError(f"max_cells must be >= 1, got {self.max_cells}")


@dataclass
class DBConfig:
    """数据库连接配置"""
    host: str = "localhost"
    port: int = 5432
    dbname: str = "region_coverer"
    user: str = "postgres"
    password: str = ""
    pool_min: int = 1
    pool_max: int = 10

    def __post_init__(self):
        if self.pool_min < 1:
            raise ValueError(f"pool_min must be >= 1, got {self.pool_min}")
        if self.pool_max < self.pool_min:
            raise ValueError(f"pool_max ({self.pool_max}) cannot be less than pool_min ({self.pool_min})")

    @property
    def dsn(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"

    def get_connection_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


@dataclass
class CryptoConfig:
    """加密配置"""
    key_path: str = ""
    key_base64: str = ""

    def get_key(self) -> bytes:
        """获取Fernet密钥字节，优先从文件读取，其次从配置读取"""
        if self.key_path and os.path.isfile(self.key_path):
            with open(self.key_path, "rb") as f:
                return f.read().strip()
        if self.key_base64:
            return self.key_base64.encode("utf-8")
        raise ValueError(
            "未配置加密密钥。请设置 CRYPTO_KEY_PATH 环境变量或 CryptoConfig.key_base64"
        )


@dataclass
class QueryConfig:
    """查询配置"""
    max_cells: int = 100
    max_db_level: int = 18

    def __post_init__(self):
        if self.max_cells < 1:
            raise ValueError(f"max_cells must be >= 1, got {self.max_cells}")
        if not (0 <= self.max_db_level <= 30):
            raise ValueError(f"max_db_level must be in [0, 30], got {self.max_db_level}")


@dataclass
class AppConfig:
    """应用全局配置"""
    s2: S2Config = field(default_factory=S2Config)
    db: DBConfig = field(default_factory=DBConfig)
    crypto: CryptoConfig = field(default_factory=CryptoConfig)
    query: QueryConfig = field(default_factory=QueryConfig)

    @classmethod
    def from_env(cls) -> "AppConfig":
        """从环境变量加载配置"""
        s2 = S2Config(
            min_level=int(os.getenv("S2_MIN_LEVEL", "12")),
            max_level=int(os.getenv("S2_MAX_LEVEL", "18")),
            max_cells=int(os.getenv("S2_MAX_CELLS", "500")),
        )
        db = DBConfig(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "region_coverer"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            pool_min=int(os.getenv("DB_POOL_MIN", "1")),
            pool_max=int(os.getenv("DB_POOL_MAX", "10")),
        )
        crypto = CryptoConfig(
            key_path=os.getenv("CRYPTO_KEY_PATH", ""),
            key_base64=os.getenv("CRYPTO_KEY_BASE64", ""),
        )
        query = QueryConfig(
            max_cells=int(os.getenv("QUERY_MAX_CELLS", "100")),
            max_db_level=int(os.getenv("QUERY_MAX_DB_LEVEL", "18")),
        )
        return cls(s2=s2, db=db, crypto=crypto, query=query)


_config_instance: AppConfig | None = None


def get_config() -> AppConfig:
    """获取全局配置实例（延迟初始化）"""
    global _config_instance
    if _config_instance is None:
        _config_instance = AppConfig.from_env()
    return _config_instance


def set_config(config: AppConfig) -> None:
    """设置全局配置实例"""
    global _config_instance
    _config_instance = config


config = AppConfig.from_env()
