"""控制分区。

门控按分区隔离放行：每个分区有自己的开门顺序（按里程由近及远）和自己的限流
（批量上限按分区各自计算），一个分区故障退出不影响其他分区按原语义运行，恢复
并回后重新参与整站调度。同站健康分区可临时合并成一个放行域，任一分区故障时
自动拆开。分区状态的唯一事实来源是这里的登记表，门控、联锁与审计都从它读。
"""

from __future__ import annotations

from .partition import ACTIVE, ISOLATED, Domain, MergeRecord, Partition, build_domains
from .registry import PartitionRegistry

__all__ = [
    "ACTIVE",
    "ISOLATED",
    "Domain",
    "MergeRecord",
    "Partition",
    "PartitionRegistry",
    "build_domains",
]
