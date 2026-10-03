"""分区登记表。

分区文档落在 ``partition/<id>``，与站台门、故障登记表使用同一个原子文档存储，
因此"分区阶段落盘"与"门区锁定落盘"拥有相同的崩溃安全保证（写临时文件再原子
替换）。
"""

from __future__ import annotations

from ..errors import RailError
from ..store import JsonStore
from .partition import Partition


class PartitionMissing(RailError):
    """分区未标定。"""

    def __init__(self, partition_id_text: str) -> None:
        super().__init__(f"分区不存在: {partition_id_text}", component="partition")
        self.partition_id = partition_id_text


class PartitionRegistry:
    """分区登记表。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store

    @staticmethod
    def _doc_id(partition_id_text: str) -> str:
        return f"partition/{partition_id_text}"

    def save(self, partition: Partition) -> Partition:
        """写入或更新一个分区。"""

        self._store.write(self._doc_id(partition.id), partition.as_dict())
        return partition

    def get(self, partition_id_text: str) -> Partition | None:
        """读取一个分区，不存在时返回 ``None``。"""

        raw = self._store.read(self._doc_id(partition_id_text))
        if not isinstance(raw, dict):
            return None
        return Partition.from_dict(raw)

    def require(self, partition_id_text: str) -> Partition:
        """读取一个分区，不存在时抛出异常。"""

        partition = self.get(partition_id_text)
        if partition is None:
            raise PartitionMissing(partition_id_text)
        return partition

    def list(self) -> list[Partition]:
        """按分区标识排序列出全部分区。"""

        found: list[Partition] = []
        for doc_id in self._store.list_ids():
            if not doc_id.startswith("partition/") or doc_id.count("/") != 1:
                continue
            raw = self._store.read(doc_id)
            if isinstance(raw, dict):
                found.append(Partition.from_dict(raw))
        return sorted(found, key=lambda item: item.id)

    def by_station(self, station_id: str) -> list[Partition]:
        """按拓扑顺序返回某站台的全部分区。"""

        return [item for item in self.list() if item.station_id == station_id]

    def clear(self) -> None:
        """删除全部分区文档（仅供重新标定使用）。"""

        for partition in self.list():
            self._store.remove(self._doc_id(partition.id))
