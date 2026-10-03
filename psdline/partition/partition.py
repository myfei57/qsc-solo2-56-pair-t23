"""分区数据结构。

分区是"站台 + 连续门区子集"的不可变拓扑，加上一个可变的运行阶段
（``active`` / ``degraded``）。拓扑（站台、门区、顺序）落盘后只在重新标定时
改变；阶段随故障置位/清除与恢复流程切换。开门顺序在分区内部仍按门区里程由近
及远，保持与整站批量开门相同的乘客语义。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..errors import InterlockError

ACTIVE = "active"
DEGRADED = "degraded"

ARRIVAL = "arrival"
DEPART = "depart"


class PartitionError(InterlockError):
    """分区处于退出（故障）态，动作被本分区拒绝。"""


@dataclass(frozen=True)
class Partition:
    """一个互相隔离的门控分区。"""

    id: str
    station_id: str
    groups: tuple[str, ...]
    phase: str = ACTIVE
    reason: str = ""

    def __post_init__(self) -> None:
        if not self.id:
            raise PartitionError("分区标识为空", component="partition")
        if not self.station_id:
            raise PartitionError("分区所属站台为空", component="partition")
        if not self.groups:
            raise PartitionError("分区至少要包含一个门区", component="partition")
        if len(set(self.groups)) != len(self.groups):
            raise PartitionError(f"分区 {self.id} 门区重复", component="partition")
        if self.phase not in (ACTIVE, DEGRADED):
            raise PartitionError(f"分区阶段非法: {self.phase}", component="partition")

    @property
    def active(self) -> bool:
        """分区是否正常参与到发。"""

        return self.phase == ACTIVE

    @property
    def degraded(self) -> bool:
        """分区是否已因故障退出。"""

        return self.phase == DEGRADED

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "station_id": self.station_id,
            "groups": list(self.groups),
            "phase": self.phase,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "Partition":
        groups = payload.get("groups", [])
        return cls(
            id=str(payload.get("id", "")),
            station_id=str(payload.get("station_id", "")),
            groups=tuple(str(item) for item in groups) if isinstance(groups, list) else (),
            phase=str(payload.get("phase", ACTIVE)),
            reason=str(payload.get("reason", "")),
        )


def partition_id(station_id: str, index: int) -> str:
    """生成确定性的分区标识（与标定顺序无关，重放稳定）。"""

    return f"{station_id}-P{index:02d}"


def partition_quota_name(partition_id_text: str, operation: str) -> str:
    """返回某分区某动作（``arrival`` / ``depart``）的配额名。"""

    if operation not in (ARRIVAL, DEPART):
        raise PartitionError(f"未知的限流动作: {operation}", component="partition")
    return f"partition/{partition_id_text}/{operation}"
