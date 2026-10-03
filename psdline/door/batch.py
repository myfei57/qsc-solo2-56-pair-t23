"""批量开门。

批量开门按放行域逐域执行：每个域（健康分区、或合并后的分区组）内部按门区里程
由近及远排序，并受自己的批量上限约束——超限时只开最近的那一批，剩下的留给下
一个周期。被联锁锁定的门区直接跳过并记进 ``skipped``，不会静默漏开；隔离分区
的整个域记为 ``partition_isolated``，其余域照常放行，互不影响。

不给放行域时退化为整站一个域（旧语义），供检修工具与旧调用方直接使用。
"""

from __future__ import annotations

from ..partition import ACTIVE, Domain
from ..station import DoorZone, open_order
from .door import DoorController


def batch_open(
    controller: DoorController,
    station_id: str,
    zones: list[DoorZone],
    domains: list[Domain] | None = None,
) -> dict:
    """按里程顺序批量开门；给定放行域时逐域独立排序、独立限流。"""

    gates = controller.gates.by_station(station_id)
    by_group: dict[str, list[str]] = {}
    for gate in gates:
        by_group.setdefault(gate.group, []).append(gate.id)
    if not domains:
        domains = [
            Domain(
                key="all",
                station_id=station_id,
                groups=tuple(open_order(zones)),
                state=ACTIVE,
                limit=controller.batch_limit(),
            )
        ]
    zone_by_id = {zone.id: zone for zone in zones}
    opened: list[str] = []
    deferred: list[str] = []
    skipped: dict[str, str] = {}
    per_domain: dict[str, dict] = {}
    for domain in domains:
        order = open_order([zone_by_id[group] for group in domain.groups if group in zone_by_id])
        order += sorted(group for group in domain.groups if group not in zone_by_id)
        if domain.state != ACTIVE:
            for group in order:
                skipped[group] = "partition_isolated"
            per_domain[domain.key] = {
                "state": domain.state,
                "opened": [],
                "deferred": [],
                "skipped": list(order),
                "limit": domain.limit,
                "merged": domain.merged,
            }
            continue
        scheduled = order[: domain.limit]
        domain_deferred = order[domain.limit :]
        deferred.extend(domain_deferred)
        domain_opened: list[str] = []
        domain_skipped: list[str] = []
        for group in scheduled:
            gate_ids = by_group.get(group, [])
            if not gate_ids:
                skipped[group] = "gate_missing"
                domain_skipped.append(group)
                continue
            if any(controller.lockouts.is_locked(gate_id) for gate_id in gate_ids):
                skipped[group] = "locked"
                domain_skipped.append(group)
                continue
            for gate_id in gate_ids:
                controller.gates.set_open(gate_id, True)
            opened.append(group)
            domain_opened.append(group)
        per_domain[domain.key] = {
            "state": domain.state,
            "opened": domain_opened,
            "deferred": domain_deferred,
            "skipped": domain_skipped,
            "limit": domain.limit,
            "merged": domain.merged,
        }
    return {
        "station_id": station_id,
        "order": open_order(zones),
        "opened": opened,
        "deferred": deferred,
        "skipped": skipped,
        "limit": controller.batch_limit(),
        "domains": per_domain,
    }
