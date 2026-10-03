"""控制分区模型。

控制分区是门控、联锁与审计共用的隔离边界：同一站台的物理门区按配置切成若干
分区，每个分区独立排序、独立限流、独立承担故障。分区状态只有两种——``ACTIVE``
（正常参与放行）与 ``ISOLATED``（故障退出，等待恢复并回）。同站全部健康的分区
可以用一条 ``MergeRecord`` 临时并成一个放行域（限流叠加、顺序统一按里程），
任一分区故障时合并立即自动拆开。

``Domain`` 是门控侧看到的放行域：一次批量开门里独立排序、独立限流的最小单位。
健康分区各自成域，合并后的分区共成一个域，隔离分区也单独成域（状态置为
``ISOLATED``，门控据此把它的门区记为跳过而不是静默漏开）。
"""

from __future__ import annotations

from dataclasses import dataclass

ACTIVE = "active"
ISOLATED = "isolated"


@dataclass(frozen=True)
class Partition:
    """一个控制分区：站台内一组物理门区的隔离单位。"""

    id: str
    station_id: str
    groups: tuple[str, ...]
    state: str = ACTIVE
    revision: int = 1
    reason: str = ""
    at: float = 0.0

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "station_id": self.station_id,
            "groups": list(self.groups),
            "state": self.state,
            "revision": self.revision,
            "reason": self.reason,
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "Partition":
        groups = payload.get("groups", [])
        return cls(
            id=str(payload.get("id", "")),
            station_id=str(payload.get("station_id", "")),
            groups=tuple(str(item) for item in groups) if isinstance(groups, list) else (),
            state=str(payload.get("state", ACTIVE)),
            revision=int(payload.get("revision", 1)),
            reason=str(payload.get("reason", "")),
            at=float(payload.get("at", 0.0)),
        )


@dataclass(frozen=True)
class MergeRecord:
    """一次临时合并：同站全部健康分区并成一个放行域。"""

    station_id: str
    members: tuple[str, ...]
    at: float

    def as_dict(self) -> dict:
        return {
            "station_id": self.station_id,
            "members": list(self.members),
            "at": self.at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "MergeRecord":
        members = payload.get("members", [])
        return cls(
            station_id=str(payload.get("station_id", "")),
            members=tuple(str(item) for item in members) if isinstance(members, list) else (),
            at=float(payload.get("at", 0.0)),
        )


@dataclass(frozen=True)
class Domain:
    """放行域：独立排序、独立限流的最小开门单位。"""

    key: str
    station_id: str
    groups: tuple[str, ...]
    state: str
    limit: int
    merged: bool = False

    def as_dict(self) -> dict:
        return {
            "key": self.key,
            "station_id": self.station_id,
            "groups": list(self.groups),
            "state": self.state,
            "limit": self.limit,
            "merged": self.merged,
        }


def build_domains(
    partitions: list[Partition],
    merge: MergeRecord | None,
    base_limit: int,
) -> list[Domain]:
    """由分区状态与合并记录推出当前放行域（纯函数）。

    合并只在全部成员都健康时生效；任一成员不健康时合并记录视为失效，各分区
    退回独立成域（合并文档由恢复流程清理，这里不改任何状态）。
    """

    ordered = sorted(partitions, key=lambda item: item.id)
    domains: list[Domain] = []
    merged_ids: set[str] = set()
    if merge is not None:
        members = [item for item in ordered if item.id in set(merge.members)]
        if members and all(item.state == ACTIVE for item in members):
            groups = tuple(group for item in members for group in item.groups)
            domains.append(
                Domain(
                    key="merge:" + "+".join(item.id for item in members),
                    station_id=members[0].station_id,
                    groups=groups,
                    state=ACTIVE,
                    limit=base_limit * len(members),
                    merged=True,
                )
            )
            merged_ids = {item.id for item in members}
    for item in ordered:
        if item.id in merged_ids:
            continue
        domains.append(
            Domain(
                key=item.id,
                station_id=item.station_id,
                groups=item.groups,
                state=item.state,
                limit=base_limit,
            )
        )
    return domains
