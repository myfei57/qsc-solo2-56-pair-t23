"""重启后的锁定状态还原。

控制服务重启后发现场门与故障登记表可能不一致，因此恢复流程以故障登记表为准：
仍置位的门重新锁上，已清除的门解锁。恢复过程逐门返回动作，便于巡检核对。
"""

from __future__ import annotations

from ..psd import PSDRegistry
from .clear import releasable
from .interlock import ACTIVE, FaultRegistry


def restore_lockouts(
    faults: FaultRegistry,
    gates: PSDRegistry,
    lockouts,
    gap_clear: bool,
    keep_locked: set[str] | None = None,
) -> dict:
    """按故障登记表还原全部门的锁定状态。

    ``keep_locked`` 里的门属于已落盘为"退出"的控制分区：即使故障登记表里没有
    置位故障（例如人工隔离或清扫失败隔离），重启后也保持锁定，把"是否并回"留给
    现场显式恢复，而不是在一次重启里悄悄解锁。
    """

    kept = keep_locked or set()
    actions: list[dict] = []
    for gate in gates.list():
        state = faults.state(gate.id)
        if state == ACTIVE:
            lockouts.set(gate.id, True)
            actions.append({"psd_id": gate.id, "action": "locked", "reason": "fault_active"})
            continue
        if gate.id in kept:
            if not gate.locked:
                lockouts.set(gate.id, True)
            actions.append({"psd_id": gate.id, "action": "kept_locked", "reason": "partition_degraded"})
            continue
        allowed, reason = releasable(gates, gate.id, gap_clear)
        if gate.locked and not allowed:
            actions.append({"psd_id": gate.id, "action": "kept_locked", "reason": reason})
            continue
        lockouts.clear(gate.id)
        actions.append({"psd_id": gate.id, "action": "released", "reason": "fault_clear"})
    return {
        "restored": len(actions),
        "locked": len([item for item in actions if item["action"] == "locked"]),
        "released": len([item for item in actions if item["action"] == "released"]),
        "actions": actions,
    }
