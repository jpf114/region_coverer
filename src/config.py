"""
全局配置：S2参数、数据库连接、加密密钥路径
"""
import os
from dataclasses import dataclass, field


@dataclass
class S2Config:
    """S2 Region Coverer 参数"""
    min_level: int = 12       # 内部最大Cell级别 ≈ 1.5km
    max_level: int = 18       # 边界最小Cell级别 ≈ 20m
    max_cells: int = 500      # 每个村落最多生成的Cell数（原代码20远远不够）


@dataclass
class DBConfig:
    """数据库连接配置"""
    host: str = "localhost"
    port: int = 5432
    dbname: str = "region_coverer"
    user: str = "postgres"
    password: str = ""

    @property
    def dsn(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"


@dataclass
class CryptoConfig:
    """加密配置"""
    key_path: str = ""        # Fernet密钥文件路径
    key_base64: str = ""      # 或直接提供Base64编码的密钥

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
class AppConfig:
    """应用全局配置"""
    s2: S2Config = field(default_factory=S2Config)
    db: DBConfig = field(default_factory=DBConfig)
    crypto: CryptoConfig = field(default_factory=CryptoConfig)

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
        )
        crypto = CryptoConfig(
            key_path=os.getenv("CRYPTO_KEY_PATH", ""),
            key_base64=os.getenv("CRYPTO_KEY_BASE64", ""),
        )
        return cls(s2=s2, db=db, crypto=crypto)


# 全局默认配置实例
config = AppConfig.from_env()
