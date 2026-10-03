"""联锁状态快照。"""

from __future__ import annotations

from ..partition import ACTIVE, Partition
from ..psd import PSDRegistry
from .interlock import FaultRegistry


def interlock_status(
    faults: FaultRegistry,
    gates: PSDRegistry,
    max_locked_ratio: float = 0.5,
    partitions: list[Partition] | None = None,
    failure_mode: str = "isolate",
) -> dict:
    """汇总故障、锁定与放行余量；给分区列表时按分区拆开判定。

    整站放行判定随故障模式走：``isolate`` 只看健康分区（隔离分区已经退出，
    不再拖累其他分区）；``degrade`` 沿用整站锁定比例（任一分区超限就卡住
    全站）。不给分区列表时保持旧的整站语义。
    """

    active = faults.active_ids()
    all_gates = gates.list()
    locked = [gate.id for gate in all_gates if gate.locked]
    total = len(all_gates)
    ratio = len(locked) / total if total else 0.0
    report = {
        "faults_active": active,
        "fault_count": len(active),
        "locked": locked,
        "locked_count": len(locked),
        "locked_ratio": round(ratio, 4),
        "max_locked_ratio": max_locked_ratio,
        "can_release_depart": ratio <= max_locked_ratio,
    }
    if partitions is None:
        return report
    per_partition: list[dict] = []
    for partition in partitions:
        members = [
            gate
            for gate in all_gates
            if gate.station == partition.station_id and gate.group in partition.groups
        ]
        locked_ids = [gate.id for gate in members if gate.locked]
        member_ratio = len(locked_ids) / len(members) if members else 0.0
        per_partition.append(
            {
                "id": partition.id,
                "station_id": partition.station_id,
                "state": partition.state,
                "groups": list(partition.groups),
                "gates": len(members),
                "locked": locked_ids,
                "locked_count": len(locked_ids),
                "locked_ratio": round(member_ratio, 4),
                "can_release": partition.state == ACTIVE and member_ratio <= max_locked_ratio,
            }
        )
    report["partitions"] = per_partition
    report["failure_mode"] = failure_mode
    if failure_mode == "isolate":
        healthy = [item for item in per_partition if item["state"] == ACTIVE]
        report["can_release_depart"] = all(item["can_release"] for item in healthy)
    return report
