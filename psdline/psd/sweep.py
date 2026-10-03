"""整站清扫。

交接班前要把门区收回关闭位并逐条记录异常。支持两种粒度：

* :func:`sweep_station` 保持"逐门清扫、某樘门写失败立刻停下"的原有语义；
* :func:`sweep_partitions` 按控制分区隔离清扫：一个分区内某樘门失败只隔离该分区
  （标记为需要退出），其他分区继续清扫并返回各自结果，避免一个坏分区拖住整站。
"""

from __future__ import annotations

from ..errors import RailError
from .psd import PSDRegistry


def sweep_station(gates: PSDRegistry, station_id: str) -> dict:
    """把某站台的全部站台门收回关闭位。"""

    targets = gates.by_station(station_id)
    closed: list[str] = []
    for gate in targets:
        if not gate.open:
            continue
        try:
            gates.set_open(gate.id, False)
        except RailError as error:
            raise RailError(
                f"清扫在门 {gate.id} 处失败: {error.message}", component="psd"
            ) from error
        closed.append(gate.id)
    return {
        "station_id": station_id,
        "closed": len(closed),
        "gates": closed,
        "scanned": len(targets),
    }


def sweep_partitions(
    gates: PSDRegistry,
    station_id: str,
    partition_groups: dict[str, list[str]] | None = None,
) -> dict:
    """按分区把站台门收回关闭位，单分区失败不波及其他分区。

    ``partition_groups`` 为 ``{分区标识: [门区, …]}``；为 ``None`` 时退化为整站
    一个分区。每个分区独立逐门清扫：区内某樘门写失败就把该分区记入
    ``failed_partitions`` 并停止本分区、继续下一个分区。
    """

    if partition_groups is None:
        partition_groups = {"STATION": sorted({gate.group for gate in gates.by_station(station_id)})}
    results: dict[str, dict] = {}
    failed: dict[str, str] = {}
    total_closed = 0
    total_scanned = 0
    for partition_id, groups in partition_groups.items():
        wanted = set(groups)
        targets = [gate for gate in gates.by_station(station_id) if gate.group in wanted]
        closed: list[str] = []
        error = ""
        for gate in targets:
            if not gate.open:
                continue
            try:
                gates.set_open(gate.id, False)
            except RailError as rail_error:
                error = f"{gate.id}: {rail_error.message}"
                break
            closed.append(gate.id)
        results[partition_id] = {
            "closed": len(closed),
            "gates": closed,
            "scanned": len(targets),
            "error": error,
        }
        total_closed += len(closed)
        total_scanned += len(targets)
        if error:
            failed[partition_id] = error
    return {
        "station_id": station_id,
        "closed": total_closed,
        "scanned": total_scanned,
        "partitions": results,
        "failed_partitions": failed,
    }
