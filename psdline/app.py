"""平台装配与控制循环。

``Platform`` 把命名空间、站台、列车、站台门、信号、联锁、门控、门区映射、间隙
探测、配额与审计按真实调用链装到一起，并暴露四件事：到发编排（到站、发车、
门区联动、批量开门、清扫）、控制循环、状态快照与手动检修动作。

控制链固定为「信号 → 停稳对位 → 开门 → 间隙探测 → 关门 → 发车 → 审计」，
任何一步错位都会被对应组件拒绝，并把拒绝原因落进审计。
"""

from __future__ import annotations

from .audit import AuditLog, build_timeline
from .clock import Clock, SystemClock
from .config import PlatformConfig, default_config, with_storage_root
from .door import (
    DoorController,
    active_group,
    batch_open,
    close_and_audit,
    depart,
    depart_policy,
    door_report,
    door_state,
    enqueue,
    open_pressure,
    open_doors,
    open_group,
    process_queue,
    queued_total,
)
from .errors import RailError
from .eventbus import Event, EventBus
from .gap import GapRegistry, gap_summary
from .ids import IdFactory
from .interlock import (
    FaultRegistry,
    InterlockArmer,
    interlock_status,
    next_sequence,
    releasable,
    restore_lockouts,
    verdict_of,
)
from .partition import Partition, PartitionManager, PartitionRegistry, partition_quota_name
from .ns import Namespace, NamespaceRegistry
from .platform import MappingRegistry, adjacent, group_keys, physical_groups, resolve
from .psd import LockoutRegistry, PSD, PSDRegistry, sweep_partitions, sweep_station
from .quota import QuotaRegistry
from .sig import SignalController, aspect_of, depart_checked, refresh_position, signal_summary
from .station import (
    DoorZone,
    Layout,
    LayoutRegistry,
    Schedule,
    ScheduleRegistry,
    Station,
    StationRegistry,
    nearest_zone,
    spread_m,
)
from .store import CommandLog, JsonStore, JsonlJournal, StoreMetrics
from .telemetry import TrendBuffer
from .train import (
    AlignRegistry,
    DoorStateRegistry,
    Dwell,
    DwellRegistry,
    PositionTracker,
    Route,
    RouteRegistry,
    Train,
    TrainRegistry,
    train_state,
)

ARRIVAL = "arrival"
DEPART = "depart"


class Platform:
    """站台门区顺序与联锁控制平台。"""

    def __init__(
        self,
        config: PlatformConfig | None = None,
        clock: Clock | None = None,
        unit: str | None = None,
    ) -> None:
        self.config = (config or default_config()).validate()
        self.clock = clock or SystemClock()
        self.bus = EventBus()
        self.unit = unit or self.config.platform.unit
        self.ids = IdFactory("op")

        store = JsonStore(self.config.storage_root)
        self.store = store
        self.metrics = StoreMetrics(store.root)
        self.audit = AuditLog(
            self.clock,
            self.bus,
            JsonlJournal(store.root, "audit", segment_limit=self.config.audit.journal_segment),
        )
        self.namespace = Namespace(
            self.config.namespace.line, list(self.config.namespace.stations)
        )
        self.namespaces = NamespaceRegistry(self.unit)
        self.trend = TrendBuffer(period_s=self.config.signal.refresh_period_s)

        self.stations = StationRegistry(store)
        self.layouts = LayoutRegistry(store)
        self.schedules = ScheduleRegistry(store)
        self.trains = TrainRegistry(store)
        self.align = AlignRegistry(store, tolerance_m=self.config.station.stop_tolerance_m)
        self.train_doors = DoorStateRegistry(store)
        self.dwells = DwellRegistry(store, max_seconds=self.config.train.dwell_max_s)
        self.positions = PositionTracker(
            store,
            max_position_m=self.config.train.max_position_m,
            stale_after_s=self.config.signal.stale_after_s,
        )
        self.routes = RouteRegistry(store)

        self.gates = PSDRegistry(store)
        self.lockouts = LockoutRegistry(self.gates)
        self.signals = SignalController(store)
        self.faults = FaultRegistry(store)
        self.armers = InterlockArmer(store)
        self.mappings = MappingRegistry(store)
        self.gaps = GapRegistry(store)
        self.quotas = QuotaRegistry(store)
        self.partition_registry = PartitionRegistry(store)
        self.commands = CommandLog(store)
        self.doors = DoorController(
            store, self.align, self.commands, self.gates, self.lockouts
        )
        self.doors.set_batch_limit(self.config.door.batch_limit)
        self.partitions = PartitionManager(
            self.partition_registry, self.quotas, self.gates, self.lockouts
        )

        self._cycles = 0
        self._operations = 0
        self._event_counts: dict[str, int] = {}
        self._partition_reconciliation: dict = {"partitions": 0, "actions": []}
        self.bus.subscribe("*", self._on_event)
        self._register_namespaces()
        self._seed(store)
        self._restore()

    def _on_event(self, event: Event) -> None:
        """统计总线事件，供状态接口判断动作有没有真的流过组件。"""

        self._event_counts[event.topic] = self._event_counts.get(event.topic, 0) + 1

    # ---- 装配 ---------------------------------------------------------

    def _register_namespaces(self) -> None:
        """登记现场设备点位（控制台与审计共用同一套名字）。"""

        for area, device, kind in (
            ("line", "signal", "signal-controller"),
            ("line", "sequence", "sequence-source"),
            ("station", "platform", "platform-screen"),
            ("station", "gap", "gap-detector"),
            ("station", "door", "platform-door"),
            ("station", "interlock", "door-interlock"),
            ("station", "quota", "quota-counter"),
            ("station", "audit", "audit-sink"),
        ):
            self.namespaces.register(area, device, kind)

    def _seed(self, store: JsonStore) -> None:
        """按线路命名空间布置站台、布局、映射与站台门。"""

        seed_doc = "platform/seed"
        if store.exists(seed_doc):
            self._ensure_partitions()
            return
        groups_per_station = self.config.station.groups_per_station
        doors_per_group = self.config.station.doors_per_group
        span = self.config.station.group_span_m
        partition_layout: dict[str, tuple[tuple[str, ...], ...]] = {}
        for station_index, station_id in enumerate(self.namespace.station_ids(), start=1):
            groups = [f"G{index:02d}" for index in range(1, groups_per_station + 1)]
            self.stations.upsert(
                Station(
                    id=station_id,
                    name=f"站台 {station_index}",
                    line=self.namespace.line_id(),
                    door_groups=groups,
                )
            )
            base = (station_index - 1) * span
            zones = [
                DoorZone(id=group, distance_m=int(base + (index - 1) * span / groups_per_station))
                for index, group in enumerate(groups, start=1)
            ]
            self.layouts.save(Layout(station_id=station_id, zones=zones))
            self.mappings.renumber(station_id, {group: group for group in groups})
            self.schedules.save(
                Schedule(station_id=station_id, headways=[120, 150, 180, 240])
            )
            for group in groups:
                for door_index in range(1, doors_per_group + 1):
                    self.gates.upsert(
                        PSD(
                            id=f"{station_id}-{group}-{door_index:02d}",
                            station=station_id,
                            group=group,
                        )
                    )
            partition_layout[station_id] = self._partition_groups(groups)
        for name, limit in (
            (ARRIVAL, self.config.quota.arrival_limit),
            (DEPART, self.config.quota.depart_limit),
        ):
            self.quotas.set_limit(name, limit)
        # 分区在站台门全部就位后标定，索引才能把每樘门归到所属分区。
        self._ensure_partitions(partition_layout)
        store.write(
            seed_doc,
            {"line": self.namespace.line_id(), "stations": self.namespace.station_ids()},
        )

    def _ensure_partitions(
        self, layout: dict[str, tuple[tuple[str, ...], ...]] | None = None
    ) -> None:
        """标定分区并补齐分区配额；已标定（升级老数据）时只补缺失的配额。"""

        if layout is None:
            layout = {
                station_id: self._partition_groups(list(self.stations.require(station_id).door_groups))
                for station_id in self.namespace.station_ids()
            }
        if not self.partitions.list():
            self.partitions.provision(layout)
        else:
            self.partitions._reload_index()
        for operation, limit in (
            (ARRIVAL, self.config.quota.arrival_limit),
            (DEPART, self.config.quota.depart_limit),
        ):
            if self.quotas.get(operation) is None:
                self.quotas.set_limit(operation, limit)
            for partition in self.partitions.list():
                name = partition_quota_name(partition.id, operation)
                if self.quotas.get(name) is None:
                    self.quotas.set_limit(name, limit)

    def _partition_groups(self, groups: list[str]) -> tuple[tuple[str, ...], ...]:
        """按配置把门区切成连续分区；``0`` 表示整站一个分区。"""

        width = self.config.partition.groups_per_partition
        if width <= 0:
            return (tuple(groups),)
        return tuple(
            tuple(groups[start : start + width]) for start in range(0, len(groups), width)
        )

    def _restore(self) -> None:
        """从落盘状态恢复操作计数、锁定状态与分区阶段。"""

        state = self.store.read("platform/state")
        if isinstance(state, dict):
            self._operations = int(state.get("operations", 0))
            self._cycles = int(state.get("cycles", 0))
        gap_clear = not self.gaps.blocked_ids()
        # 已落盘为退出态的分区，其门在重启还原里一律保持锁定，不做自动并回。
        self.partitions._reload_index()
        degraded_gate_ids: set[str] = set()
        for partition in self.partitions.degraded_partitions():
            for gate in self.gates.by_station(partition.station_id):
                if gate.group in partition.groups:
                    degraded_gate_ids.add(gate.id)
        restore_lockouts(
            self.faults,
            self.gates,
            self.lockouts,
            gap_clear,
            keep_locked=degraded_gate_ids,
        )
        reconciliation = self.partitions.reconcile(set(self.faults.active_ids()), gap_clear)
        self._partition_reconciliation = reconciliation
        for action in reconciliation["actions"]:
            if action["action"] in ("held_degraded", "kept_degraded"):
                self.audit.record(
                    "partition_reconcile",
                    "",
                    "",
                    partition=action["partition"],
                    outcome=action["action"],
                )

    def _persist(self) -> None:
        """落盘平台计数，供重启后对照。"""

        self.store.write(
            "platform/state",
            {
                "operations": self._operations,
                "cycles": self._cycles,
                "unit": self.unit,
                "line": self.namespace.line_id(),
            },
        )

    def _record_operation(self, op_id: str, operation: str, report: dict) -> dict:
        """把编排步骤逐条写事件，并记一条汇总审计。"""

        train_id = str(report.get("train_id", ""))
        station_id = str(report.get("station_id", ""))
        reserved = {
            "train_id",
            "station_id",
            "op_id",
            "severity",
            "message",
            "component",
            "action",
            "detail",
        }
        for step in report.get("steps", []):
            detail = {
                key: value
                for key, value in dict(step.get("detail", {})).items()
                if not isinstance(value, (dict, list)) and key not in reserved
            }
            self.bus.publish(
                Event.of(
                    "door",
                    str(step.get("component", "door")),
                    str(step.get("action", "step")),
                    op_id=op_id,
                    train_id=train_id,
                    severity="notice",
                    **detail,
                )
            )
        for violation in report.get("violations", []):
            self.bus.publish(
                Event.of(
                    "audit",
                    "interlock",
                    "interlock_rejected",
                    op_id=op_id,
                    train_id=train_id,
                    station_id=station_id,
                    severity="alarm",
                    message=f"{operation} 被拒绝：{violation}",
                    violation=violation,
                )
            )
        entry = self.audit.record(
            f"{operation}_complete",
            train_id,
            station_id,
            op_id=op_id,
            ok=bool(report.get("ok")),
            violations=list(report.get("violations", [])),
        )
        return {
            "op_id": op_id,
            "operation": operation,
            "ok": bool(report.get("ok")),
            "violations": list(report.get("violations", [])),
            "audit_id": entry.id,
        }

    # ---- 生命周期 -----------------------------------------------------

    def start(self) -> dict:
        """启动平台：核对命名空间、站台、布局与映射是否一致。"""

        missing: dict[str, list[str]] = {}
        for station_id in self.namespace.station_ids():
            if self.stations.get(station_id) is None:
                missing.setdefault("station", []).append(station_id)
            if self.layouts.get(station_id) is None:
                missing.setdefault("layout", []).append(station_id)
            if self.mappings.current(station_id) is None:
                missing.setdefault("mapping", []).append(station_id)
        return {
            "unit": self.unit,
            "line": self.namespace.label(),
            "missing": missing,
            "stations": len(self.namespace.station_ids()),
            "gates": len(self.gates.list()),
            "operations": self._operations,
            "ready": not missing,
        }

    def stop(self) -> dict:
        """停运平台：整站清扫后收起全部灯位。"""

        op_id = self.ids.labeled("stop")
        steps: list[dict] = []
        closed = 0
        for station_id in self.namespace.station_ids():
            report = self.sweep(station_id)
            closed += report["closed"]
            steps.append(
                {
                    "component": "psd",
                    "action": "sweep",
                    "detail": {"station_id": station_id, "closed": report["closed"]},
                }
            )
        for train in self.trains.list():
            self.signals.clear(train.id, ARRIVAL)
            self.signals.clear(train.id, DEPART)
        report = {"ok": True, "train_id": "", "closed": closed, "steps": steps, "violations": []}
        summary = self._record_operation(op_id, "stop", report)
        self._operations += 1
        self._persist()
        return {**report, **summary}

    # ---- 到发编排 -----------------------------------------------------

    def arrive(self, train_id: str, station_id: str, position_m: int) -> dict:
        """到站到开门：信号到站 → 停稳对位 → 对位落盘 → 分区开门。

        放行按分区互相隔离：仅正常分区参与开门并消费各自的到发限流配额；退出分区
        跳过（门区保持锁定），其他分区照常开门。分区配额在持锁区间内申请，任一正常
        分区配额不足则整单拒绝并补偿退回；对位/开门抛错时同样退回本次已消费配额。
        """

        op_id = self.ids.labeled("arrive")
        steps: list[dict] = []
        violations: list[str] = []

        def reject(reason: str) -> dict:
            violations.append(reason)
            self.bus.publish(
                Event.of(
                    "audit",
                    "interlock",
                    "arrive_rejected",
                    op_id=op_id,
                    train_id=train_id,
                    station_id=station_id,
                    severity="alarm",
                    message=reason,
                )
            )
            return {
                "ok": False,
                "train_id": train_id,
                "station_id": station_id,
                "steps": steps,
                "violations": violations,
            }

        if not self.quotas.consume(ARRIVAL):
            return reject("到站配额已用完")
        try:
            self.namespace.require(station_id)
        except RailError as error:
            self._refund_global(ARRIVAL)
            return reject(error.message)
        if self.stations.get(station_id) is None:
            self._refund_global(ARRIVAL)
            return reject(f"站台未登记: {station_id}")

        partition_ids = [item.id for item in self.partitions.by_station(station_id)]
        admitted: dict = {}
        with self.partitions.guard(partition_ids):
            admitted = self.partitions.admit(station_id, ARRIVAL)
            if not admitted["allowed"]:
                self._refund_global(ARRIVAL)
                if admitted["reason"] == "all_partitions_degraded":
                    return reject("该站台全部分区故障退出，无法接车")
                return reject(
                    f"分区 {admitted['partition']} 到站限流已用完"
                    if admitted["reason"] == "quota_exhausted"
                    else f"分区 {admitted['partition']} 到站限流未配置"
                )

            def refund_all() -> None:
                self.partitions.refund(station_id, ARRIVAL)
                self._refund_global(ARRIVAL)

            self.signals.raise_signal(train_id, ARRIVAL, self.clock.now())
            steps.append(
                {"component": "sig", "action": "arrival_signal", "detail": {"train_id": train_id}}
            )
            if self.trains.get(train_id) is None:
                self.trains.upsert(Train(id=train_id, line=self.namespace.line_id()))
            self.trains.assign_station(train_id, station_id)

            stop_position = self._stop_position(station_id)
            try:
                alignment = self.align.align(train_id, position_m, stop_position, self.clock.now())
            except RailError as error:
                refund_all()
                return reject(error.message)
            steps.append(
                {
                    "component": "train",
                    "action": "align",
                    "detail": {"train_id": train_id, "position_m": alignment.position_m},
                }
            )
            try:
                opened = self.doors_open(train_id, station_id)
            except RailError as error:
                refund_all()
                return reject(error.message)
        entry = self.audit.record(ARRIVAL, train_id, station_id, position_m=position_m)
        if admitted.get("degraded"):
            self.audit.record(
                "partition_skipped",
                train_id,
                station_id,
                operation=ARRIVAL,
                partitions=",".join(admitted["degraded"]),
            )
        report = {
            "ok": True,
            "train_id": train_id,
            "station_id": station_id,
            "position_m": alignment.position_m,
            "opened_partitions": admitted["partitions"],
            "degraded_partitions": admitted["degraded"],
            "opened_groups": opened["groups"],
            "steps": steps,
            "violations": [],
        }
        summary = self._record_operation(op_id, "arrive", report)
        self._operations += 1
        self._persist()
        return {**report, **summary, "arrival_audit": entry.id}

    def doors_open(self, train_id: str, station_id: str) -> dict:
        """开门并确认车门：对位必须先落盘。

        只开正常分区的门区，且在每个分区内部按里程由近及远；退出分区的门区处于
        联锁锁定，直接跳过并在 ``skipped`` 里逐条给出原因，不静默漏开，也不会因为
        故障分区有锁而让正常分区开不了门。
        """

        report = open_doors(self.doors, train_id, self.clock.now())
        layout = self.layouts.get(station_id)
        ordered = [zone.id for zone in layout.zones] if layout else []
        by_partition = self.partitions.ordered_groups(station_id, ordered)
        opened_groups: list[str] = []
        skipped: dict[str, str] = {}
        degraded = [item.id for item in self.partitions.degraded_partitions(station_id)]
        for partition in self.partitions.active_partitions(station_id):
            for group in by_partition.get(partition.id, []):
                resolved = self.mappings.resolve(station_id, group)
                gate_ids = [
                    gate.id
                    for gate in self.gates.by_station(station_id)
                    if gate.group == resolved
                ]
                if not gate_ids:
                    skipped[group] = "gate_missing"
                    continue
                if any(self.lockouts.is_locked(gate_id) for gate_id in gate_ids):
                    skipped[group] = "locked"
                    continue
                open_group(self.doors, self.mappings, station_id, group)
                opened_groups.append(group)
        for partition in self.partitions.degraded_partitions(station_id):
            for group in by_partition.get(partition.id, []):
                skipped.setdefault(group, "partition_degraded")
        self.train_doors.confirm_open(train_id, self.clock.now())
        self.bus.publish(
            Event.of(
                "door",
                "door",
                "opened",
                train_id=train_id,
                station_id=station_id,
                severity="notice",
                group=opened_groups[0] if opened_groups else "",
            )
        )
        return {
            **report,
            "group": opened_groups[0] if opened_groups else "",
            "groups": opened_groups,
            "skipped": skipped,
            "degraded_partitions": degraded,
        }

    def depart(self, train_id: str, station_id: str) -> dict:
        """关门到发车：分区限流 → 关门指令 → 间隙探测 → 关门落盘 → 发车信号。

        发车同样按分区隔离：每个正常分区消费各自的发车配额并在发车期间锁定本区
        一个代表门；退出分区不参与（其门本就锁在关闭位）。后续关门/间隙/信号任一
        步失败，本次已消费的整站级与分区级配额全部补偿退回，分区代表门保持锁定
        （安全侧，等下一次发车或恢复流程处理）；成功后才解锁代表门。
        """

        op_id = self.ids.labeled("depart")
        steps: list[dict] = []

        def fail(violations: list[str]) -> dict:
            report = {
                "ok": False,
                "train_id": train_id,
                "station_id": station_id,
                "steps": steps,
                "violations": violations,
            }
            summary = self._record_operation(op_id, "depart", report)
            return {**report, **summary}

        if not self.quotas.consume(DEPART):
            return fail(["发车配额已用完"])

        partition_ids = [item.id for item in self.partitions.by_station(station_id)]
        locked_ids: list[str] = []
        admitted: dict = {}
        with self.partitions.guard(partition_ids):
            admitted = self.partitions.admit(station_id, DEPART)
            if not admitted["allowed"]:
                self._refund_global(DEPART)
                if admitted["reason"] == "all_partitions_degraded":
                    return fail(["该站台全部分区故障退出，无法发车"])
                return fail([f"分区 {admitted['partition']} 发车限流已用完"])

            def refund_all() -> None:
                self.partitions.refund(station_id, DEPART)
                self._refund_global(DEPART)

            # 每个正常分区锁一个代表门（区内里程最近的那樘），发车成功后统一解锁。
            for partition in self.partitions.active_partitions(station_id):
                gate = self._first_gate(station_id, partition.groups)
                if gate is not None and not self.lockouts.is_locked(gate.id):
                    self.lockouts.set(gate.id, True)
                    locked_ids.append(gate.id)
                    steps.append(
                        {"component": "interlock", "action": "lock", "detail": {"psd_id": gate.id}}
                    )
            try:
                closed = close_and_audit(
                    self.doors, self.audit, train_id, station_id, DEPART, self.clock.now()
                )
            except RailError as error:
                refund_all()
                return fail([error.message])
            steps.append(
                {"component": "door", "action": "close_durable", "detail": {"train_id": train_id}}
            )
            gap_state = self.gaps.state(train_id)
            steps.append(
                {
                    "component": "gap",
                    "action": "check",
                    "detail": {"train_id": train_id, "state": gap_state},
                }
            )
            verdict = depart_checked(self.signals, self.store, self.gaps, train_id, self.clock.now())
            if not verdict["ok"]:
                refund_all()
                return fail(list(verdict["violations"]))
            steps.append(
                {"component": "sig", "action": "depart_signal", "detail": {"train_id": train_id}}
            )
            departure = depart(self.doors, train_id, self.clock.now())
            for gate_id in locked_ids:
                self.lockouts.clear(gate_id)
        if admitted.get("degraded"):
            self.audit.record(
                "partition_skipped",
                train_id,
                station_id,
                operation=DEPART,
                partitions=",".join(admitted["degraded"]),
            )
        report = {
            "ok": True,
            "train_id": train_id,
            "station_id": station_id,
            "close_durable": bool(closed["close_durable"]),
            "aspect": verdict["aspect"],
            "cleared_commands": departure["cleared_commands"],
            "partitions": admitted["partitions"],
            "degraded_partitions": admitted["degraded"],
            "steps": steps,
            "violations": [],
        }
        summary = self._record_operation(op_id, "depart", report)
        self._operations += 1
        self._persist()
        return {**report, **summary}

    def check_gap(self, train_id: str, clear: bool = True) -> dict:
        """确认间隙无异常，或登记一次可疑探测结果。"""

        if clear:
            result = self.gaps.check(train_id, self.clock.now())
        else:
            result = self.gaps.probe(train_id, False, self.clock.now())
        self.audit.record("gap_probe", train_id, "", clear=clear)
        return {"ok": True, "train_id": train_id, "clear": result.clear, "violations": []}

    def renumber(self, station_id: str, groups: dict[str, str]) -> dict:
        """按最新布局重排门区映射，并按新映射联动一次。"""

        mapping = self.mappings.renumber(station_id, groups)
        first_key = sorted(groups)[0]
        linked = open_group(self.doors, self.mappings, station_id, first_key)
        self.audit.record("mapping_renumber", "", station_id, revision=mapping.revision)
        return {
            "ok": True,
            "station_id": station_id,
            "revision": mapping.revision,
            "mapping": mapping.as_dict(),
            "active_group": linked["group"],
            "violations": [],
        }

    def enqueue(self, train_id: str, command_id: str, kind: str, station_id: str = "") -> dict:
        """把一条门控指令排进队列。"""

        command = enqueue(self.doors, train_id, command_id, kind, station_id)
        self.audit.record(
            "door_enqueue", train_id, station_id, command_id=command.id, command_kind=kind
        )
        return {"ok": True, "train_id": train_id, "command": command.as_dict(), "violations": []}

    def process_queue(self, train_id: str) -> dict:
        """返回列车队列里待执行的门控指令，并带上发车自检结果。"""

        policy = depart_policy(self.doors, train_id)
        ids = process_queue(self.doors, train_id)
        self.audit.record("door_process", train_id, "", queued=len(ids))
        return {
            "ok": True,
            "train_id": train_id,
            "ids": ids,
            "ready": policy["allowed"],
            "violations": list(policy["failures"]),
        }

    def clear_fault(self, psd_id: str) -> dict:
        """清除门区故障并按条件解锁。"""

        gate = self.gates.require(psd_id)
        allowed, reason = releasable(self.gates, psd_id, True)
        if not allowed:
            return {"ok": False, "psd_id": psd_id, "locked": True, "violations": [reason]}
        self.faults.clear_fault(self.lockouts, psd_id, self.clock.now())
        self.audit.record("interlock_clear", "", gate.station, psd_id=psd_id)
        return {"ok": True, "psd_id": psd_id, "locked": False, "violations": []}

    def set_fault(self, psd_id: str) -> dict:
        """置位门区故障并锁定。"""

        gate = self.gates.require(psd_id)
        self.faults.set_fault(self.lockouts, psd_id, self.clock.now())
        self.audit.record("interlock_fault", "", gate.station, psd_id=psd_id)
        return {"ok": True, "psd_id": psd_id, "locked": True, "violations": []}

    def isolate_partition(self, partition_id_text: str, reason: str = "manual") -> dict:
        """把一个控制分区退出：落盘退出态并锁定本区全部门。

        其他分区的到发、限流、开门顺序都不经过该分区，立即继续正常运行；退出
        分区的门区在到发里被跳过。这是"只让故障分区退出"的显式入口。
        """

        partition = self.partitions.degrade(partition_id_text, reason)
        self.audit.record(
            "partition_isolate",
            "",
            partition.station_id,
            partition=partition.id,
            groups=",".join(partition.groups),
            reason=reason,
        )
        return {
            "ok": True,
            "partition": partition.as_dict(),
            "degraded": partition.degraded,
            "violations": [],
        }

    def recover_partition(self, partition_id_text: str) -> dict:
        """把一个退出分区并回正常态。

        条件与解锁单樘门一致：本区门已回到关闭位、且间隙无异常。条件不满足时
        分区维持退出并返回原因，不做半套并回。
        """

        gap_clear = not self.gaps.blocked_ids()
        partition = self.partitions.require(partition_id_text)
        allowed, reason = self.partitions.can_recover(partition, gap_clear)
        if not allowed:
            return {
                "ok": False,
                "partition": partition.as_dict(),
                "degraded": True,
                "violations": [reason],
            }
        recovered = self.partitions.recover(partition_id_text, gap_clear, reason="manual_recover")
        self.audit.record(
            "partition_recover",
            "",
            recovered.station_id,
            partition=recovered.id,
            groups=",".join(recovered.groups),
        )
        return {
            "ok": True,
            "partition": recovered.as_dict(),
            "degraded": False,
            "violations": [],
        }

    def set_partition_quota(
        self, partition_id_text: str, operation: str, limit: int
    ) -> dict:
        """单独调整某分区的到/发限流上限。"""

        if operation not in (ARRIVAL, DEPART):
            return {"ok": False, "violations": [f"未知限流动作: {operation}"]}
        self.partitions.set_partition_quota(partition_id_text, operation, limit)
        self.audit.record(
            "partition_quota",
            "",
            "",
            partition=partition_id_text,
            operation=operation,
            limit=limit,
        )
        return {"ok": True, "partition": partition_id_text, "operation": operation, "limit": limit}

    def arm(self, train_id: str, armed: bool) -> dict:
        """启用或退出一列车的联锁。"""

        state = self.armers.arm(train_id, armed)
        self.audit.record("interlock_arm", train_id, "", armed=state)
        return {"ok": True, "train_id": train_id, "armed": state, "violations": []}

    def refresh_position(self, train_id: str, position_m: int) -> dict:
        """按信号刷新列车位置。"""

        outcome = refresh_position(self.positions, train_id, position_m, self.clock.now())
        if outcome["ok"]:
            self.trend.append("train_position_m", self.clock.now(), position_m)
        else:
            self.audit.record("position_rejected", train_id, "", reason=outcome["reason"])
        return {**outcome, "violations": [] if outcome["ok"] else [outcome["reason"]]}

    def sequence_verdict(self, previous: int, step: int) -> dict:
        """推进门控事件序号并给出回绕判定。"""

        current = next_sequence(previous, step)
        verdict = verdict_of(previous, current)
        self.audit.record(
            "sequence_verdict",
            "",
            "",
            previous=verdict.previous,
            current=verdict.current,
            advanced=verdict.advanced,
        )
        return {"ok": verdict.advanced, "verdict": verdict.as_dict(), "violations": [] if verdict.advanced else [verdict.reason]}

    def batch_open(self, station_id: str) -> dict:
        """按布局里程顺序批量开门。

        开门顺序仍由布局里程决定；跳过原因按分区细化：处于退出态分区的门区记为
        ``partition_degraded``（整区退出，与单樘门的 ``locked`` 区分），其余被联锁
        锁定的门区维持 ``locked``。退出分区不影响正常分区被打开。
        """

        layout = self.layouts.get(station_id)
        zones = list(layout.zones) if layout else []
        outcome = batch_open(self.doors, station_id, zones)
        nearest = nearest_zone(zones)
        degraded_groups = {
            group
            for partition in self.partitions.degraded_partitions(station_id)
            for group in partition.groups
        }
        skipped: dict[str, str] = {}
        for group, reason in outcome["skipped"].items():
            skipped[group] = (
                "partition_degraded"
                if reason == "locked" and group in degraded_groups
                else reason
            )
        by_partition = self.partitions.ordered_groups(station_id, outcome["order"])
        partition_result = {
            partition.id: {
                "phase": partition.phase,
                "opened": [g for g in by_partition.get(partition.id, []) if g in outcome["opened"]],
                "skipped": [g for g in by_partition.get(partition.id, []) if g in skipped],
            }
            for partition in self.partitions.by_station(station_id)
        }
        self.audit.record(
            "batch_open",
            "",
            station_id,
            opened=len(outcome["opened"]),
            skipped=len(skipped),
            degraded=len(degraded_groups),
        )
        return {
            "ok": not skipped,
            "station_id": station_id,
            "order": outcome["order"],
            "opened": outcome["opened"],
            "deferred": outcome["deferred"],
            "skipped": skipped,
            "nearest": nearest.id if nearest else "",
            "partitions": partition_result,
            "violations": [],
        }

    def sweep(self, station_id: str) -> dict:
        """整站清扫，按控制分区隔离：坏分区退出，好分区照常收回。

        某个分区里有门写失败时，把该分区隔离（落盘退出态、记审计），其余分区
        继续清扫。全部分区都成功时返回 ``ok=True``；有分区被隔离时
        ``ok=False`` 并在 ``failed_partitions`` 里给出每樘失败门的原因。
        """

        partition_groups = {
            item.id: list(item.groups) for item in self.partitions.by_station(station_id)
        }
        result = sweep_partitions(self.gates, station_id, partition_groups)
        for partition_id in result["failed_partitions"]:
            self.partitions.degrade(
                partition_id, f"sweep_failed: {result['failed_partitions'][partition_id]}"
            )
            self.audit.record(
                "partition_isolate",
                "",
                station_id,
                partition=partition_id,
                reason="sweep_failed",
                detail=result["failed_partitions"][partition_id],
            )
        if result["failed_partitions"]:
            self.audit.record(
                "sweep_partial",
                "",
                station_id,
                closed=result["closed"],
                failed=",".join(sorted(result["failed_partitions"])),
            )
        else:
            self.audit.record("sweep", "", station_id, closed=result["closed"])
        return {
            "ok": not result["failed_partitions"],
            **result,
        }

    def register_dwell(self, train_id: str, seconds: float, station_id: str = "") -> dict:
        """登记停站时长。"""

        dwell = self.dwells.set(Dwell(train_id=train_id, seconds=seconds, station_id=station_id))
        return {"ok": True, "dwell": dwell.as_dict(), "violations": []}

    def assign_route(self, train_id: str, origin: str, destination: str, distance_m: int) -> dict:
        """登记运行交路。"""

        route = self.routes.assign(
            Route(train_id=train_id, origin=origin, destination=destination, distance_m=distance_m)
        )
        return {"ok": True, "route": route.__dict__, "violations": []}

    # ---- 控制循环 -----------------------------------------------------

    def cycle(self, dt: float = 1.0) -> dict:
        """推进一个控制周期，返回本周期摘要。"""

        if dt <= 0:
            return {"ok": False, "violations": ["步长必须为正"], "cycle": self._cycles}
        self._cycles += 1
        moment = self.clock.now()
        stale = [
            train.id for train in self.trains.list() if self.positions.stale(train.id, moment)
        ]
        interlock = interlock_status(self.faults, self.gates, self.config.interlock.max_locked_ratio)
        partition_summary = self.partitions.summary()
        holding = [
            train.id
            for train in self.trains.list()
            if not self.armers.armed(train.id) and depart_policy(self.doors, train.id)["allowed"]
        ]
        self.trend.append("locked_count", moment, interlock["locked_count"])
        self.trend.append("degraded_partitions", moment, partition_summary["degraded"])
        self.trend.append("queued_commands", moment, queued_total(self.doors))
        self.trend.append("audit_entries", moment, len(self.audit.list()))
        self._persist()
        return {
            "ok": True,
            "cycle": self._cycles,
            "dt": dt,
            "stale_positions": stale,
            "locked_count": interlock["locked_count"],
            "can_release_depart": interlock["can_release_depart"],
            "degraded_partitions": [
                item["id"] for item in partition_summary["partitions"] if item["phase"] == "degraded"
            ],
            "ready_trains": holding,
            "gaps": gap_summary(self.gaps, [train.id for train in self.trains.list()]),
            "violations": [],
        }

    # ---- 查询 ---------------------------------------------------------

    def status(self) -> dict:
        """汇总状态快照，供控制台与命令行读取。"""

        trains = self.trains.list()
        ids = [train.id for train in trains]
        return {
            "platform": self.config.name,
            "unit": self.unit,
            "line": self.namespace.label(),
            "operations": self._operations,
            "cycles": self._cycles,
            "utc": self.clock.utc_iso(),
            "namespace": self.namespace.as_dict(),
            "namespaces": self.namespaces.describe(),
            "open_gates": self.gates.count_open(),
            "lockout_changes": self.lockouts.changes,
            "signals_raised": self.signals.raises,
            "signals_cleared": self.signals.clears,
            "interlock_counters": {
                "faults_set": self.faults.sets,
                "faults_cleared": self.faults.clears,
                "armed": self.armers.arms,
                "disarmed": self.armers.disarms,
                "armed_trains": self.armers.armed_trains(),
            },
            "gap_engine": {"checks": self.gaps.checks, "probes": self.gaps.probes},
            "event_counts": dict(sorted(self._event_counts.items())),
            "console_hits": len(self.bus.history("console")),
            "stations": [station.as_dict() for station in self.stations.list()],
            "trains": {train.id: train_state(train.id, self.align, self.train_doors, self.positions) for train in trains},
            "doors": [
                {**gate.as_dict(), "open": gate.open, "locked": gate.locked}
                for gate in self.gates.list()
            ],
            "door_report": door_report(self.doors, self.namespace.station_ids()),
            "signals": signal_summary(self.signals, ids),
            "interlock": interlock_status(
                self.faults, self.gates, self.config.interlock.max_locked_ratio
            ),
            "gaps": gap_summary(self.gaps, ids),
            "quota": {
                ARRIVAL: self.quotas.status(ARRIVAL),
                DEPART: self.quotas.status(DEPART),
            },
            "partitions": self.partitions.summary(),
            "store": self.metrics.summary(),
            "audit": self.audit.summary(),
            "trend": self.trend.snapshot(limit=4),
            "ids_issued": self.ids.issued(),
        }

    def snapshot(self) -> dict:
        """返回只读的整体快照（状态 + 命名空间 + 门区映射 + 审计时间线）。"""

        return {
            "status": self.status(),
            "groups": {
                station_id: group_keys(self.mappings, station_id)
                for station_id in self.namespace.station_ids()
            },
            "physical_groups": {
                station_id: physical_groups(self.mappings, station_id)
                for station_id in self.namespace.station_ids()
            },
            "mapping_revisions": {
                station_id: {
                    "latest": self.mappings.history(station_id).latest(),
                    "log": self.mappings.history(station_id).as_dict()["revisions"],
                }
                for station_id in self.namespace.station_ids()
            },
            "adjacent": self._adjacent_sample(),
            "timeline": build_timeline(self.audit.list(), limit=20),
        }

    def train_view(self, train_id: str) -> dict:
        """返回一列车的综合视图。"""

        return {
            "train": self.trains.get(train_id).as_dict() if self.trains.get(train_id) else None,
            "state": train_state(train_id, self.align, self.train_doors, self.positions),
            "doors": door_state(self.doors, train_id),
            "aspect": aspect_of(self.signals, train_id),
            "gap": self.gaps.state(train_id),
        }

    def station_view(self, station_id: str) -> dict:
        """返回一个站台的综合视图。"""

        layout = self.layouts.get(station_id)
        mapping = self.mappings.current(station_id)
        zones = list(layout.zones) if layout else []
        schedule = self.schedules.get(station_id)
        return {
            "station": self.stations.get(station_id).as_dict() if self.stations.get(station_id) else None,
            "group_count": self.stations.group_count(station_id),
            "zones": [zone.as_dict() for zone in zones],
            "order": [zone.id for zone in zones],
            "zone_distances": {
                zone.id: self.layouts.zone_distance(station_id, zone.id) for zone in zones
            },
            "span_m": spread_m(zones),
            "minimum_headway_s": self.schedules.minimum_headway(station_id) if schedule else 0,
            "revision": mapping.revision if mapping else 0,
            "groups": group_keys(self.mappings, station_id),
            "active_group": active_group(self.doors, station_id),
            "pressure": open_pressure(self.doors, station_id),
            "partitions": [
                {
                    "id": item.id,
                    "phase": item.phase,
                    "groups": list(item.groups),
                    "order": self.partitions.ordered_groups(
                        station_id, [zone.id for zone in zones]
                    ).get(item.id, []),
                    "reason": item.reason,
                }
                for item in self.partitions.by_station(station_id)
            ],
        }

    def resolve_group(self, station_id: str, group_key: str) -> dict:
        """解析一个逻辑门区。"""

        group = resolve(self.mappings, station_id, group_key)
        return {
            "station_id": station_id,
            "group_key": group_key,
            "group": group,
            "neighbours": adjacent(self.mappings, station_id, group_key),
        }

    def note_error(self, error: RailError) -> dict:
        """把平台异常写入审计，返回落盘记录。"""

        entry = self.audit.record(
            "error",
            "",
            "",
            component=error.component,
            message=error.message,
            error_type=type(error).__name__,
        )
        return entry.as_dict()

    def reset_state(self) -> bool:
        """清掉落盘的平台计数（交付前重新标定用）。"""

        if not self.store.exists("platform/state"):
            return False
        return self.store.remove("platform/state")

    # ---- 内部 ---------------------------------------------------------

    def _refund_global(self, name: str) -> None:
        """退回一次整站级配额（失败补偿用），已为零时不动。"""

        quota = self.quotas.get(name)
        if quota is not None and quota.used > 0:
            self.quotas.set_used(name, quota.used - 1)

    def _stop_position(self, station_id: str) -> int:
        """返回站台的停稳位置（取最近门区里程）。"""

        layout = self.layouts.get(station_id)
        zones = list(layout.zones) if layout else []
        nearest = nearest_zone(zones)
        return nearest.distance_m if nearest else 0

    def _first_gate(self, station_id: str, groups: tuple[str, ...] | list[str]) -> PSD | None:
        """返回给定门区集合里里程最近的一樘门（分区代表门）。"""

        ordered = set(groups)
        candidates = [
            gate for gate in self.gates.by_station(station_id) if gate.group in ordered
        ]
        layout = self.layouts.get(station_id)
        if layout is not None:
            distance = {zone.id: zone.distance_m for zone in layout.zones}
            candidates.sort(key=lambda gate: distance.get(gate.group, 1 << 30))
        return candidates[0] if candidates else None

    def _adjacent_sample(self) -> dict:
        """给快照提供一个相邻关系样例。"""

        station_ids = self.namespace.station_ids()
        if not station_ids:
            return {}
        station_id = station_ids[0]
        keys = group_keys(self.mappings, station_id)
        if not keys:
            return {}
        return {keys[0]: adjacent(self.mappings, station_id, keys[0])}


def build_platform(
    config: PlatformConfig | None = None,
    clock: Clock | None = None,
    storage_root: str | None = None,
    unit: str | None = None,
) -> Platform:
    """构造一个平台；``storage_root`` 可覆盖配置里的存储目录。"""

    base = config or default_config()
    if storage_root is not None:
        base = with_storage_root(base, storage_root)
    return Platform(base, clock=clock, unit=unit)
