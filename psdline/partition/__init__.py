"""控制分区。

门控按"控制分区"互相隔离：一个分区有自己的门区开门顺序、自己的到发限流配额和
自己的故障态。一个分区故障只让该分区退出（其门区锁定、该分区的到发被拒绝），
其他分区继续按原语义运行；故障清除并满足解锁条件后分区自动并回，无需停运。

分区拓扑在配置期（停运标定）确定，运行期不支持临时合并：合并会重新引入跨区
共享锁与共享配额，破坏"单区故障不波及邻区"的不变量。需要更大的放行面时，在
配置里调整每分区门区数后重新标定。

落盘事实只有一份：每个分区一个 ``partition/<id>`` 文档，故障态先落盘再动作，
保证崩溃后重启仍能按故障登记表与分区文档把现场还原成"故障区保持退出"。
"""

from __future__ import annotations

from .manager import PartitionManager
from .partition import (
    ACTIVE,
    DEGRADED,
    Partition,
    PartitionError,
    partition_id,
    partition_quota_name,
)
from .registry import PartitionRegistry

__all__ = [
    "ACTIVE",
    "DEGRADED",
    "Partition",
    "PartitionError",
    "PartitionManager",
    "PartitionRegistry",
    "partition_id",
    "partition_quota_name",
]
