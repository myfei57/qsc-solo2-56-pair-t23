"""分区运行管理：隔离、限流、故障退出与并回。

管理器是分区唯一的运行期入口，负责四件互相牵制的事：

* **开门顺序**：分区内的门区仍按里程由近及远，顺序由布局推导，不在分区里另存
  一份（避免布局变更后两份事实漂移）。
* **限流**：每个分区对到站/发车各有独立配额（``partition/<id>/arrival``、
  ``partition/<id>/depart``）。整站级配额仍由平台保留为跨区总闸；分区配额只负责
  "本区不被打爆"。一次到发涉及的多个分区按分区标识升序加锁后逐区消费，任一分区
  配额不足即整体拒绝，并把已消费的配额逐一补偿退回，做到"全有或全无"。
* **故障退出**：门区故障会把该门所在分区标记为退出（先落盘分区阶段，再锁本区
  全部门），退出分区的到发被拒绝、门保持锁定；其他分区的配额、顺序、锁都不经过
  退出分区，因此不受影响。
* **并回**：故障全部清除、门已回关闭位、间隙无异常后分区自动恢复；也可由人工
  ``isolate`` / ``recover`` 显式退出/并回。阶段切换先落盘，崩溃重启后由
  :meth:`reconcile` 按故障登记表与现场门态还原。

并发模型：每个分区一把可重入锁，跨分区操作按分区标识升序一次性获取所涉分区锁，
保证多列车并发放行不同分区时互不阻塞，同时杜绝多区操作的加锁环路。
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

from ..errors import RailError
from ..psd import LockoutRegistry, PSDRegistry
from ..quota import QuotaRegistry
from .partition import (
    ACTIVE,
    ARRIVAL,
    DEGRADED,
    DEPART,
    Partition,
    PartitionError,
    partition_quota_name,
)
from .registry import PartitionRegistry


class PartitionManager:
    """分区拓扑、阶段、限流与隔离动作的统一入口。"""

    def __init__(
        self,
        registry: PartitionRegistry,
        quotas: QuotaRegistry,
        gates: PSDRegistry,
        lockouts: LockoutRegistry,
    ) -> None:
        self._registry = registry
        self._quotas = quotas
        self._gates = gates
        self._lockouts = lockouts
        self._gate_to_partition: dict[str, str] = {}
        self._locks: dict[str, threading.RLock] = {}
        self._topology = threading.RLock()
        self._reload_index()

    # ---- 标定 ---------------------------------------------------------

    def provision(self, layout: dict[str, tuple[str, ...]]) -> None:
        """按 ``{站台: (门区, …)}`` 重建分区拓扑。

        每个站台的门区按传入顺序（即里程由近及远）切成连续子段，分段数由调用方
        在分区方案里给定。已有的分区阶段尽量保留（同站台同序号的分区沿用旧阶段），
        这样重新标定不会把一个仍在退出态的分区悄悄放回正常。
        """

        with self._topology:
            previous = {(item.station_id, item.id): item for item in self._registry.list()}
            self._registry.clear()
            index: dict[str, int] = {}
            for station_id in sorted(layout):
                for groups in layout[station_id]:
                    seq = index.get(station_id, 0) + 1
                    index[station_id] = seq
                    from .partition import partition_id

                    pid = partition_id(station_id, seq)
                    prior = previous.get((station_id, pid))
                    phase = prior.phase if prior is not None else ACTIVE
                    reason = prior.reason if prior is not None else ""
                    self._registry.save(
                        Partition(
                            id=pid,
                            station_id=station_id,
                            groups=tuple(groups),
                            phase=phase,
                            reason=reason,
                        )
                    )
            self._reload_index()

    def set_partition_quota(self, partition_id_text: str, operation: str, limit: int) -> None:
        """单独调整某分区某动作的配额。"""

        self._registry.require(partition_id_text)
        self._quotas.set_limit(partition_quota_name(partition_id_text, operation), limit)

    def _reload_index(self) -> None:
        self._gate_to_partition = {}
        self._locks = {}
        for partition in self._registry.list():
            self._locks.setdefault(partition.id, threading.RLock())
            for group in partition.groups:
                for gate in self._gates.by_station(partition.station_id):
                    if gate.group == group:
                        self._gate_to_partition[gate.id] = partition.id

    # ---- 查询 ---------------------------------------------------------

    def list(self) -> list[Partition]:
        return self._registry.list()

    def by_station(self, station_id: str) -> list[Partition]:
        return self._registry.by_station(station_id)

    def require(self, partition_id_text: str) -> Partition:
        return self._registry.require(partition_id_text)

    def of_group(self, station_id: str, group: str) -> Partition | None:
        """返回某站台某门区所属的分区。"""

        for partition in self._registry.by_station(station_id):
            if group in partition.groups:
                return partition
        return None

    def ordered_groups(self, station_id: str, order: list[str]) -> dict[str, list[str]]:
        """把布局给定的里程顺序按分区归类，返回 ``{分区: 门区顺序}``。"""

        grouped: dict[str, list[str]] = {
            item.id: [] for item in self._registry.by_station(station_id)
        }
        for group in order:
            partition = self.of_group(station_id, group)
            if partition is not None:
                grouped.setdefault(partition.id, []).append(group)
        return grouped

    def active_partitions(self, station_id: str) -> list[Partition]:
        """站台当前正常参与到发的分区。"""

        return [item for item in self._registry.by_station(station_id) if item.active]

    def degraded_partitions(self, station_id: str | None = None) -> list[Partition]:
        """当前退出的分区；可按站台过滤。"""

        items = self._registry.list()
        if station_id is not None:
            items = [item for item in items if item.station_id == station_id]
        return [item for item in items if item.degraded]

    def _lock_for(self, partition_id_text: str) -> threading.RLock:
        with self._topology:
            lock = self._locks.get(partition_id_text)
            if lock is None:
                lock = threading.RLock()
                self._locks[partition_id_text] = lock
            return lock

    @contextmanager
    def guard(self, partition_ids: list[str]) -> Iterator[None]:
        """按分区标识升序获取一组分区锁，供一次到发串行化所涉分区。"""

        ordered = sorted(set(partition_ids))
        acquired = [self._lock_for(pid) for pid in ordered]
        for lock in acquired:
            lock.acquire()
        try:
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()

    # ---- 阶段切换（落盘在前，崩溃安全） -------------------------------

    def _save_phase(self, partition: Partition, phase: str, reason: str) -> Partition:
        updated = Partition(
            id=partition.id,
            station_id=partition.station_id,
            groups=partition.groups,
            phase=phase,
            reason=reason,
        )
        return self._registry.save(updated)

    def _partition_gate_ids(self, partition: Partition) -> list[str]:
        gate_ids: list[str] = []
        for gate in self._gates.by_station(partition.station_id):
            if gate.group in partition.groups:
                gate_ids.append(gate.id)
        return gate_ids

    def degrade(self, partition_id_text: str, reason: str) -> Partition:
        """把分区切到退出态并锁定本区全部门（故障隔离）。"""

        with self._lock_for(partition_id_text):
            partition = self._registry.require(partition_id_text)
            if partition.degraded:
                return partition
            # 先落盘退出态：即便随后锁门过程中崩溃，重启也会把本区按退出态锁住。
            partition = self._save_phase(partition, DEGRADED, reason)
            for gate_id in self._partition_gate_ids(partition):
                if not self._lockouts.is_locked(gate_id):
                    self._lockouts.set(gate_id, True)
            return partition

    def can_recover(self, partition: Partition, gap_clear: bool) -> tuple[bool, str]:
        """判定分区能否并回：无残留故障、门已关闭、间隙无异常。"""

        for gate_id in self._partition_gate_ids(partition):
            gate = self._gates.get(gate_id)
            if gate is None:
                return False, "gate_missing"
            if gate.open:
                return False, "gate_open"
        if not gap_clear:
            return False, "gap_blocked"
        return True, "ok"

    def recover(self, partition_id_text: str, gap_clear: bool, reason: str = "manual") -> Partition:
        """把分区并回正常态；条件不满足时保持退出并抛出原因。"""

        with self._lock_for(partition_id_text):
            partition = self._registry.require(partition_id_text)
            allowed, why = self.can_recover(partition, gap_clear)
            if not allowed:
                raise PartitionError(
                    f"分区 {partition_id_text} 暂不能并回: {why}", component="partition"
                )
            if partition.active:
                return partition
            # 先落盘正常态，再解锁本区门；崩溃在中间只会让门多锁一会儿（安全侧）。
            partition = self._save_phase(partition, ACTIVE, "")
            for gate_id in self._partition_gate_ids(partition):
                if self._lockouts.is_locked(gate_id):
                    self._lockouts.clear(gate_id)
            return partition

    # ---- 限流（分区独立 + 失败补偿） ----------------------------------

    def admit(self, station_id: str, operation: str) -> dict:
        """为一次到发向该站台**正常分区**申请配额。

        退出（故障）分区不参与本次放行，在 ``degraded`` 中列出由调用方跳过；其他
        正常分区逐一消费配额，任一正常分区配额不足即整体拒绝并补偿退回已消费配额，
        做到"正常分区全有或全无"。若全部分区都退出则拒绝（本区已无可用门区）。
        调用方应在 :meth:`guard` 区间内调用，使"查阶段 + 消费配额"对同一分区原子。
        """

        partitions = self._registry.by_station(station_id)
        if not partitions:
            raise RailError(f"站台 {station_id} 尚未标定分区", component="partition")
        healthy = [item for item in partitions if item.active]
        degraded = [item.id for item in partitions if item.degraded]
        if not healthy:
            return {
                "allowed": False,
                "reason": "all_partitions_degraded",
                "degraded": degraded,
                "consumed": [],
            }
        consumed: list[str] = []
        for partition in healthy:
            name = partition_quota_name(partition.id, operation)
            quota = self._quotas.get(name)
            if quota is None:
                self._refund(consumed)
                return {
                    "allowed": False,
                    "reason": "quota_unconfigured",
                    "partition": partition.id,
                    "degraded": degraded,
                    "consumed": consumed,
                }
            if not self._quotas.consume(name):
                self._refund(consumed)
                return {
                    "allowed": False,
                    "reason": "quota_exhausted",
                    "partition": partition.id,
                    "remaining": quota.remaining,
                    "degraded": degraded,
                    "consumed": consumed,
                }
            consumed.append(name)
        return {
            "allowed": True,
            "partitions": [item.id for item in healthy],
            "degraded": degraded,
            "consumed": consumed,
        }

    def _refund(self, names: list[str]) -> None:
        """把一次失败申请已消费的配额按已落盘用量减一退回。"""

        for name in names:
            quota = self._quotas.get(name)
            if quota is not None and quota.used > 0:
                self._quotas.set_used(name, quota.used - 1)

    def refund(self, station_id: str, operation: str) -> list[str]:
        """显式退回一次到发在各分区消费的配额（后续编排失败时调用）。"""

        names = [
            partition_quota_name(item.id, operation)
            for item in self._registry.by_station(station_id)
        ]
        self._refund(names)
        return names

    # ---- 重启对账 ------------------------------------------------------

    def reconcile(self, active_faults: set[str], gap_clear: bool) -> dict:
        """重启后按故障登记表与现场门态还原分区阶段。

        规则偏向安全侧：**重启只"保持退出 / 补锁"，绝不自动并回**——并回需要现场
        确认（人工 :meth:`recover`），避免一次重启把一个仍在处理中的故障分区悄悄
        放回正常。

        * 区内仍有置位故障：分区保持退出，并把本区门补锁；
        * 分区处于退出但故障已清：维持退出，等人工并回；
        * 分区正常：核对门态（本应不锁），记为 ``active``。
        """

        actions: list[dict] = []
        for partition in self._registry.list():
            gate_ids = self._partition_gate_ids(partition)
            faults_here = sorted(set(gate_ids) & active_faults)
            with self._lock_for(partition.id):
                current = self._registry.require(partition.id)
                if faults_here:
                    if current.active:
                        current = self._save_phase(current, DEGRADED, "fault_active_on_boot")
                    for gate_id in gate_ids:
                        if not self._lockouts.is_locked(gate_id):
                            self._lockouts.set(gate_id, True)
                    actions.append(
                        {"partition": partition.id, "action": "held_degraded", "faults": faults_here}
                    )
                elif current.degraded:
                    # 退出态不因重启而解除：维持现场，等显式并回。
                    actions.append({"partition": partition.id, "action": "kept_degraded"})
                else:
                    actions.append({"partition": partition.id, "action": "active"})
        return {"partitions": len(actions), "actions": actions}

    # ---- 状态汇总 ------------------------------------------------------

    def summary(self) -> dict:
        """返回分区快照（阶段、门区顺序、限流余量、锁定数）。"""

        result: list[dict] = []
        for partition in self._registry.list():
            gate_ids = self._partition_gate_ids(partition)
            locked = sum(1 for gid in gate_ids if self._lockouts.is_locked(gid))
            entry = {
                "id": partition.id,
                "station_id": partition.station_id,
                "groups": list(partition.groups),
                "phase": partition.phase,
                "reason": partition.reason,
                "gate_count": len(gate_ids),
                "locked_count": locked,
                "quota": {},
            }
            for operation in (ARRIVAL, DEPART):
                name = partition_quota_name(partition.id, operation)
                quota = self._quotas.get(name)
                entry["quota"][operation] = None if quota is None else quota.as_dict()
            result.append(entry)
        return {
            "count": len(result),
            "active": sum(1 for item in result if item["phase"] == ACTIVE),
            "degraded": sum(1 for item in result if item["phase"] == DEGRADED),
            "partitions": result,
        }
