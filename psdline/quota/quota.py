"""配额登记与消费。"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..errors import RailError
from ..store import JsonStore


@dataclass(frozen=True)
class Quota:
    """一份配额。"""

    name: str
    limit: int
    used: int

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "limit": self.limit,
            "used": self.used,
            "remaining": self.remaining,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "Quota":
        return cls(
            name=str(payload.get("name", "")),
            limit=int(payload.get("limit", 0)),
            used=int(payload.get("used", 0)),
        )


class QuotaRegistry:
    """配额登记表，落在 ``quota/<name>`` 文档上。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store
        self._lock = threading.RLock()

    @staticmethod
    def _doc_id(name: str) -> str:
        return f"quota/{name}"

    def set_limit(self, name: str, limit: int) -> Quota:
        """设置（或重置）一份配额。"""

        if not name:
            raise RailError("配额名称为空", component="quota")
        if limit <= 0:
            raise RailError("配额上限必须为正", component="quota")
        quota = Quota(name=name, limit=limit, used=0)
        self._store.write(self._doc_id(name), quota.as_dict())
        return quota

    def get(self, name: str) -> Quota | None:
        """读取一份配额，未配置时返回 ``None``。"""

        raw = self._store.read(self._doc_id(name))
        if not isinstance(raw, dict):
            return None
        return Quota.from_dict(raw)

    def consume(self, name: str) -> bool:
        """尝试消费一次配额，返回是否获准。

        读-判-写在同一把锁内原子完成：并发到发即便落到同一份配额（整站总闸或同一
        分区），也不会因交错读写出超发。
        """

        with self._lock:
            quota = self.get(name)
            if quota is None:
                raise RailError(f"配额未配置: {name}", component="quota")
            if quota.used >= quota.limit:
                return False
            updated = Quota(name=quota.name, limit=quota.limit, used=quota.used + 1)
            self._store.write(self._doc_id(name), updated.as_dict())
            return True

    def remaining(self, name: str) -> int:
        """返回剩余配额。"""

        quota = self.get(name)
        if quota is None:
            raise RailError(f"配额未配置: {name}", component="quota")
        return quota.remaining

    def set_used(self, name: str, used: int) -> Quota:
        """直接设置已用计数（供并发放行失败后的配额补偿退回）。"""

        with self._lock:
            quota = self.get(name)
            if quota is None:
                raise RailError(f"配额未配置: {name}", component="quota")
            if used < 0 or used > quota.limit:
                raise RailError("配额已用计数越界", component="quota")
            updated = Quota(name=quota.name, limit=quota.limit, used=used)
            self._store.write(self._doc_id(name), updated.as_dict())
            return updated

    def reset(self, name: str) -> Quota | None:
        """把配额已用计数清零。"""

        quota = self.get(name)
        if quota is None:
            return None
        updated = Quota(name=quota.name, limit=quota.limit, used=0)
        self._store.write(self._doc_id(name), updated.as_dict())
        return updated

    def status(self, name: str) -> dict:
        """返回配额状态快照。"""

        quota = self.get(name)
        if quota is None:
            return {"name": name, "configured": False}
        return {"configured": True, **quota.as_dict()}
