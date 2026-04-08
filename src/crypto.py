"""
Fernet加密封装：几何数据的加密/解密
"""
from cryptography.fernet import Fernet
from shapely import wkb

from .config import CryptoConfig


class GeometryCrypto:
    """几何数据加解密器"""

    def __init__(self, config: CryptoConfig):
        key = config.get_key()
        self._fernet = Fernet(key)

    def encrypt_wkb(self, geom_wkb: bytes) -> bytes:
        """加密WKB格式的几何数据"""
        return self._fernet.encrypt(geom_wkb)

    def decrypt_to_wkb(self, encrypted: bytes) -> bytes:
        """解密得到WKB字节"""
        return self._fernet.decrypt(encrypted)

    def encrypt_geometry(self, geom: "shapely.Geometry") -> bytes:
        """加密Shapely几何对象 → 加密WKB"""
        geom_wkb = geom.wkb
        return self.encrypt_wkb(geom_wkb)

    def decrypt_to_geometry(self, encrypted: bytes) -> "shapely.Geometry":
        """解密 → Shapely几何对象"""
        geom_wkb = self.decrypt_to_wkb(encrypted)
        return wkb.loads(geom_wkb)

    @staticmethod
    def generate_key() -> bytes:
        """生成新的Fernet密钥（用于初始化，非运行时调用）"""
        return Fernet.generate_key()

    @staticmethod
    def save_key(key: bytes, path: str) -> None:
        """将密钥保存到文件"""
        with open(path, "wb") as f:
            f.write(key)
