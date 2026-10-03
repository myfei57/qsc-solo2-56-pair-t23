"""控制分区登记与状态机。

分区指派与状态都落在存储上：指派文档 ``partition/<分区标识>`` 只在首次播种或
旧状态目录迁移时创建，之后绝不重写（指派不可变）；状态转换（隔离、并回）在原
文档上推进修订号，历史轨迹只进审计流水，不回写旧状态。合并记录落在
``partition_merge/<站台>``，任一分区退出时由联锁侧删除。

所有方法都可重入：重复隔离一个已隔离的分区直接返回现状（``changed=False``），
调用方据此决定要不要补审计，保证"每次真实转换恰好一条审计"。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from ..errors import PartitionError
from ..psd import PSD, PSDRegistry
from ..store import JsonStore
from .partition import ACTIVE, ISOLATED, Domain, MergeRecord, Partition, build_domains

if TYPE_CHECKING:
    from ..interlock import FaultRegistry


class PartitionRegistry:
    """控制分区登记表。"""

    def __init__(self, store: JsonStore) -> None:
        self._store = store
        self._lock = threading.RLock()

    @staticmethod
    def _doc_id(partition_id: str) -> str:
        return f"partition/{partition_id}"

    @staticmethod
    def _merge_doc_id(station_id: str) -> str:
        return f"partition_merge/{station_id}"

    # ---- 指派 ---------------------------------------------------------

    def ensure(self, station_id: str, groups: list[str], size: int) -> list[Partition]:
        """把站台门区切成固定大小的分区；已存在的指派原样保留。

        幂等：播种、重启、旧状态目录迁移都走这里。已有分区覆盖的门区不再重排，
        只给没覆盖到的门区补建分区，保证指派历史不被回写。
        """

        if size <= 0:
            raise PartitionError("每分区门区数必须为正", component="partition")
        with self._lock:
            existing = {item.id: item for item in self.list(station_id)}
            covered = {group for item in existing.values() for group in item.groups}
            missing = [group for group in groups if group not in covered]
            index = len(existing) + 1
            for start in range(0, len(missing), size):
                chunk = missing[start : start + size]
                partition_id = f"{station_id}-P{index:02d}"
                while partition_id in existing:
                    index += 1
                    partition_id = f"{station_id}-P{index:02d}"
                partition = Partition(id=partition_id, station_id=station_id, groups=tuple(chunk))
                self._store.write(self._doc_id(partition_id), partition.as_dict())
                existing[partition_id] = partition
                index += 1
            return [existing[key] for key in sorted(existing)]

    # ---- 查询 ---------------------------------------------------------

    def get(self, partition_id: str) -> Partition | None:
        """读取一个分区，不存在时返回 ``None``。"""

        raw = self._store.read(self._doc_id(partition_id))
        if not isinstance(raw, dict):
            return None
        return Partition.from_dict(raw)

    def require(self, partition_id: str) -> Partition:
        """读取一个分区，不存在时抛出异常。"""

        partition = self.get(partition_id)
        if partition is None:
            raise PartitionError(f"控制分区不存在: {partition_id}", component="partition")
        return partition

    def list(self, station_id: str | None = None) -> list[Partition]:
        """按标识排序列出分区；给站台标识时只列该站台的。"""

        found: list[Partition] = []
        for doc_id in self._store.list_ids():
            if not doc_id.startswith("partition/") or doc_id.count("/") != 1:
                continue
            raw = self._store.read(doc_id)
            if not isinstance(raw, dict):
                continue
            partition = Partition.from_dict(raw)
            if station_id is not None and partition.station_id != station_id:
                continue
            found.append(partition)
        return sorted(found, key=lambda item: item.id)

    def of_group(self, station_id: str, group: str) -> Partition | None:
        """返回一个物理门区所属的分区。"""

        for partition in self.list(station_id):
            if group in partition.groups:
                return partition
        return None

    def of_gate(self, gate: PSD) -> Partition | None:
        """返回一樘站台门所属的分区。"""

        return self.of_group(gate.station, gate.group)

    def active_merge(self, station_id: str) -> MergeRecord | None:
        """读取站台当前的合并记录，没有时返回 ``None``。"""

        raw = self._store.read(self._merge_doc_id(station_id))
        if not isinstance(raw, dict):
            return None
        return MergeRecord.from_dict(raw)

    def domains(self, station_id: str, base_limit: int) -> list[Domain]:
        """返回站台当前的放行域（合并生效时折叠成一个域）。"""

        with self._lock:
            return build_domains(self.list(station_id), self.active_merge(station_id), base_limit)

    # ---- 状态转换 -----------------------------------------------------

    def _transition(self, partition_id: str, state: str, reason: str, at: float) -> tuple[Partition, bool]:
        """推进一次状态转换；状态未变时原样返回。"""

        with self._lock:
            partition = self.require(partition_id)
            if partition.state == state:
                return partition, False
            updated = Partition(
                id=partition.id,
                station_id=partition.station_id,
                groups=partition.groups,
                state=state,
                revision=partition.revision + 1,
                reason=reason,
                at=at,
            )
            self._store.write(self._doc_id(partition_id), updated.as_dict())
            return updated, True

    def isolate(self, partition_id: str, reason: str, at: float) -> tuple[Partition, bool]:
        """把分区置为隔离（故障退出），返回（分区，是否真的变了）。"""

        return self._transition(partition_id, ISOLATED, reason, at)

    def recover(self, partition_id: str, at: float) -> tuple[Partition, bool]:
        """把分区并回正常放行，返回（分区，是否真的变了）。"""

        return self._transition(partition_id, ACTIVE, "", at)

    # ---- 临时合并 -----------------------------------------------------

    def merge(self, station_id: str, at: float) -> MergeRecord:
        """把站台全部健康分区并成一个放行域。"""

        with self._lock:
            if self.active_merge(station_id) is not None:
                raise PartitionError(f"站台 {station_id} 已存在合并", component="partition")
            partitions = self.list(station_id)
            members = tuple(item.id for item in partitions if item.state == ACTIVE)
            if len(partitions) < 2 or len(members) != len(partitions):
                raise PartitionError(
                    f"站台 {station_id} 仅全部分区都健康时才能合并", component="partition"
                )
            record = MergeRecord(station_id=station_id, members=members, at=at)
            self._store.write(self._merge_doc_id(station_id), record.as_dict())
            return record

    def split_merge(self, station_id: str) -> MergeRecord | None:
        """拆掉站台的合并，返回被拆掉的记录；没有合并时返回 ``None``。"""

        with self._lock:
            record = self.active_merge(station_id)
            if record is None:
                return None
            self._store.remove(self._merge_doc_id(station_id))
            return record

    # ---- 恢复对账 -----------------------------------------------------

    def reconcile(
        self,
        faults: "FaultRegistry",
        gates: PSDRegistry,
        at: float,
    ) -> list[dict]:
        """按故障登记表对账分区状态，返回实际发生的转换（供调用方补审计）。

        重启后分区状态以故障表为准：成员门仍有置位故障的分区必须处于隔离态，
        否则补隔离；合并记录里任一成员不健康时拆掉合并。只补不撤——没有故障
        的隔离分区保持隔离，并回必须走带条件检查的恢复路径。
        """

        actions: list[dict] = []
        with self._lock:
            gate_list = gates.list()
            station_ids = sorted({item.station_id for item in self.list()})
            for station_id in station_ids:
                for partition in self.list(station_id):
                    members = [
                        gate
                        for gate in gate_list
                        if gate.station == station_id and gate.group in partition.groups
                    ]
                    active_faults = [
                        gate.id for gate in members if faults.is_active(gate.id)
                    ]
                    if active_faults and partition.state != ISOLATED:
                        self.isolate(partition.id, "restore_reconcile", at)
                        actions.append(
                            {
                                "action": "isolated",
                                "partition": partition.id,
                                "station_id": station_id,
                                "faults": active_faults,
                            }
                        )
                merge = self.active_merge(station_id)
                if merge is not None:
                    states = {item.id: item.state for item in self.list(station_id)}
                    if any(states.get(member) != ACTIVE for member in merge.members):
                        self.split_merge(station_id)
                        actions.append(
                            {
                                "action": "split",
                                "station_id": station_id,
                                "members": list(merge.members),
                                "reason": "member_not_active",
                            }
                        )
        return actions
