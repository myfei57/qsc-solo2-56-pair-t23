"""控制分区：隔离放行、部分恢复、临时合并、崩溃安全与历史不可变。"""

from __future__ import annotations

import threading
from dataclasses import replace

import pytest

from psdline.app import build_platform
from psdline.config import (
    NamespaceSection,
    PartitionSection,
    StationSection,
    config_from_mapping,
    config_to_mapping,
    default_config,
)
from psdline.door import batch_open, open_group
from psdline.errors import ConfigError, PartitionError
from psdline.partition import ACTIVE, ISOLATED, PartitionRegistry, build_domains
from psdline.store import JsonStore


def _gates_of(platform, station_id, group):
    return [gate for gate in platform.gates.by_station(station_id) if gate.group == group]


def _first_gate(platform, station_id="ST1", group="G01"):
    return _gates_of(platform, station_id, group)[0]


def _reopen(platform, tmp_path, clock):
    """在同一个存储目录上重建平台（模拟控制服务重启）。"""

    return build_platform(default_config(), clock=clock, storage_root=str(tmp_path / "state"))


# ---- 播种与指派 ---------------------------------------------------------


def test_partitions_seeded_per_station(platform):
    partitions = platform.partitions.list("ST1")
    assert [item.id for item in partitions] == ["ST1-P01", "ST1-P02"]
    assert partitions[0].groups == ("G01", "G02")
    assert partitions[1].groups == ("G03", "G04")
    assert all(item.state == ACTIVE for item in partitions)
    assert platform.partitions.of_group("ST1", "G03").id == "ST1-P02"


def test_partition_assignment_is_stable_across_restart(platform, tmp_path, clock):
    before = {item.id: item.groups for item in platform.partitions.list()}
    platform.set_fault(_first_gate(platform).id)
    reopened = _reopen(platform, tmp_path, clock)
    after = {item.id: item.groups for item in reopened.partitions.list()}
    assert after == before
    # 重启后隔离状态与故障事实一起还原，不会悄悄并回。
    assert reopened.partitions.get("ST1-P01").state == ISOLATED


# ---- 故障隔离 -----------------------------------------------------------


def test_fault_isolates_only_its_partition(platform):
    gate = _first_gate(platform, group="G01")
    outcome = platform.set_fault(gate.id)
    assert outcome["partition"]["isolated"] == "ST1-P01"
    assert platform.partitions.get("ST1-P01").state == ISOLATED
    assert platform.partitions.get("ST1-P02").state == ACTIVE
    # 批量开门：故障分区整域跳过，健康分区按原语义放行。
    result = platform.batch_open("ST1")
    assert result["skipped"] == {"G01": "partition_isolated", "G02": "partition_isolated"}
    assert sorted(result["opened"]) == ["G03", "G04"]
    assert result["domains"]["ST1-P01"]["state"] == ISOLATED
    assert result["domains"]["ST1-P02"]["opened"] == ["G03", "G04"]
    # 门区联动对隔离分区直接拒绝。
    with pytest.raises(PartitionError):
        open_group(platform.doors, platform.mappings, "ST1", "G01", platform.partitions)


def test_isolation_is_idempotent_and_audited_once(platform):
    gate = _first_gate(platform).id
    platform.set_fault(gate)
    platform.set_fault(gate)
    kinds = [entry.kind for entry in platform.audit.list()]
    assert kinds.count("partition_isolated") == 1
    assert platform.partitions.get("ST1-P01").revision == 2


def test_arrive_opens_only_healthy_partition(platform):
    platform.set_fault(_first_gate(platform, group="G01").id)
    arrival = platform.arrive("T01", "ST1", 0)
    assert arrival["ok"] is True
    gates = platform.gates.by_station("ST1")
    assert all(gate.open for gate in gates if gate.group == "G03")
    assert not any(gate.open for gate in gates if gate.group in ("G01", "G02"))


def test_arrive_rejected_when_all_partitions_isolated(platform):
    platform.set_fault(_first_gate(platform, group="G01").id)
    platform.set_fault(_first_gate(platform, group="G03").id)
    rejected = platform.arrive("T01", "ST1", 0)
    assert rejected["ok"] is False
    assert rejected["violations"]


def test_interlock_status_scopes_release_to_healthy_partitions(platform):
    platform.set_fault(_first_gate(platform).id)
    status = platform.status()["interlock"]
    assert status["failure_mode"] == "isolate"
    assert status["can_release_depart"] is True
    per = {item["id"]: item for item in status["partitions"]}
    assert per["ST1-P01"]["state"] == ISOLATED
    assert per["ST1-P01"]["can_release"] is False
    assert per["ST1-P02"]["can_release"] is True


def test_degrade_mode_keeps_site_wide_semantics(tmp_path, clock):
    config = replace(
        default_config(),
        namespace=NamespaceSection(stations=("ST1",)),
        station=StationSection(groups_per_station=2, doors_per_group=1),
        partition=PartitionSection(failure_mode="degrade"),
    )
    platform = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"))
    gates = platform.gates.by_station("ST1")
    platform.set_fault(gates[0].id)
    # degrade：分区不退出，整站锁定比例说了算。
    assert platform.partitions.get("ST1-P01").state == ACTIVE
    assert platform.status()["interlock"]["can_release_depart"] is True
    platform.set_fault(gates[1].id)
    assert platform.status()["interlock"]["can_release_depart"] is False


# ---- 部分失败恢复 -------------------------------------------------------


def test_recovery_is_partial_until_last_fault_clears(platform):
    first = _first_gate(platform, group="G01").id
    second = _first_gate(platform, group="G02").id
    platform.set_fault(first)
    platform.set_fault(second)
    step = platform.clear_fault(first)
    assert step["ok"] is True
    assert step["recovery"]["recovered"] is False
    assert step["recovery"]["blockers"] == {second: "fault_active"}
    assert platform.partitions.get("ST1-P01").state == ISOLATED
    done = platform.clear_fault(second)
    assert done["recovery"] == {"recovered": True, "partition": "ST1-P01"}
    assert platform.partitions.get("ST1-P01").state == ACTIVE


def test_recovery_blocked_by_open_gate_then_manual_recover(platform):
    platform.batch_open("ST1")
    gate = _first_gate(platform, group="G01")
    platform.gates.set_open(gate.id, False)
    platform.set_fault(gate.id)
    cleared = platform.clear_fault(gate.id)
    assert cleared["ok"] is True
    assert cleared["recovery"]["recovered"] is False
    assert "gate_open" in cleared["recovery"]["blockers"].values()
    blocked = platform.recover_partition("ST1-P01")
    assert blocked["ok"] is False
    assert blocked["blockers"]
    platform.sweep("ST1")
    recovered = platform.recover_partition("ST1-P01")
    assert recovered["ok"] is True and recovered["changed"] is True
    assert platform.partitions.get("ST1-P01").state == ACTIVE
    again = platform.recover_partition("ST1-P01")
    assert again["ok"] is True and again["changed"] is False


def test_manual_isolate_and_recover_roundtrip(platform):
    outcome = platform.isolate_partition("ST1-P02", reason="maintenance")
    assert outcome["changed"] is True
    result = platform.batch_open("ST1")
    assert sorted(result["opened"]) == ["G01", "G02"]
    recovered = platform.recover_partition("ST1-P02")
    assert recovered["ok"] is True
    kinds = [entry.kind for entry in platform.audit.list()]
    assert "partition_isolated" in kinds and "partition_recovered" in kinds
    with pytest.raises(PartitionError):
        platform.recover_partition("ST1-P99")


# ---- 临时合并 -----------------------------------------------------------


def test_merge_unites_order_and_limit_then_fault_splits(platform):
    platform.doors.set_batch_limit(1)
    unmerged = platform.batch_open("ST1")
    assert sorted(unmerged["opened"]) == ["G01", "G03"]
    platform.sweep("ST1")
    merged = platform.merge_partitions("ST1")
    assert merged["ok"] is True
    assert merged["merge"]["members"] == ["ST1-P01", "ST1-P02"]
    again = platform.merge_partitions("ST1")
    assert again["ok"] is False
    combined = platform.batch_open("ST1")
    # 合并域限流叠加（1+1=2），顺序按里程统一：G01、G02 先于 G03。
    assert combined["opened"] == ["G01", "G02"]
    platform.sweep("ST1")
    platform.set_fault(_first_gate(platform, group="G01").id)
    assert platform.partitions.active_merge("ST1") is None
    assert platform.partitions.get("ST1-P01").state == ISOLATED
    after = platform.batch_open("ST1")
    assert after["opened"] == ["G03"]
    kinds = [entry.kind for entry in platform.audit.list()]
    assert kinds.index("partition_merged") < kinds.index("partition_split")
    assert kinds.index("partition_split") < kinds.index("partition_isolated")


def test_merge_requires_all_partitions_healthy(platform):
    platform.set_fault(_first_gate(platform).id)
    refused = platform.merge_partitions("ST1")
    assert refused["ok"] is False
    assert platform.partitions.active_merge("ST1") is None
    split = platform.split_merge("ST1")
    assert split["ok"] is False


def test_merge_disabled_by_config(tmp_path, clock):
    config = replace(
        default_config(), partition=PartitionSection(allow_merge=False)
    )
    platform = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"))
    refused = platform.merge_partitions("ST1")
    assert refused["ok"] is False
    assert refused["violations"] == ["配置禁止临时合并"]


def test_build_domains_falls_back_when_member_unhealthy():
    from psdline.partition import MergeRecord, Partition

    partitions = [
        Partition(id="ST1-P01", station_id="ST1", groups=("G01",), state=ISOLATED),
        Partition(id="ST1-P02", station_id="ST1", groups=("G02",)),
    ]
    stale_merge = MergeRecord(station_id="ST1", members=("ST1-P01", "ST1-P02"), at=0.0)
    domains = build_domains(partitions, stale_merge, base_limit=2)
    # 成员不健康时合并记录视为失效，各分区退回独立成域。
    assert [domain.key for domain in domains] == ["ST1-P01", "ST1-P02"]
    healthy = build_domains(
        [partitions[0], partitions[1]],
        None,
        base_limit=2,
    )
    assert healthy[1].limit == 2


# ---- 崩溃安全 -----------------------------------------------------------


def test_restart_reconciles_partition_state_from_fault_table(platform, tmp_path, clock):
    gate = _first_gate(platform).id
    # 模拟崩溃窗口：故障已落盘、门已锁，但分区隔离还没发生。
    platform.faults.set_fault(platform.lockouts, gate, platform.clock.now())
    assert platform.partitions.get("ST1-P01").state == ACTIVE
    reopened = _reopen(platform, tmp_path, clock)
    partition = reopened.partitions.get("ST1-P01")
    assert partition.state == ISOLATED
    assert partition.reason == "restore_reconcile"
    assert reopened.lockouts.is_locked(gate) is True
    kinds = [entry.kind for entry in reopened.audit.list()]
    assert "partition_isolated" in kinds


def test_restart_drops_merge_with_unhealthy_member(platform, tmp_path, clock):
    assert platform.merge_partitions("ST1")["ok"] is True
    gate = _first_gate(platform).id
    # 崩溃窗口：合并还在，但成员故障只落了故障表。
    platform.faults.set_fault(platform.lockouts, gate, platform.clock.now())
    reopened = _reopen(platform, tmp_path, clock)
    assert reopened.partitions.active_merge("ST1") is None
    assert reopened.partitions.get("ST1-P01").state == ISOLATED
    kinds = [entry.kind for entry in reopened.audit.list()]
    assert "partition_split" in kinds


def test_recovered_partition_rejoins_after_restart(platform, tmp_path, clock):
    gate = _first_gate(platform).id
    platform.set_fault(gate)
    platform.clear_fault(gate)
    assert platform.partitions.get("ST1-P01").state == ACTIVE
    reopened = _reopen(platform, tmp_path, clock)
    assert reopened.partitions.get("ST1-P01").state == ACTIVE
    result = reopened.batch_open("ST1")
    assert sorted(result["opened"]) == ["G01", "G02", "G03", "G04"]


# ---- 历史不可变 ---------------------------------------------------------


def test_partition_history_is_append_only(platform):
    gate = _first_gate(platform).id
    platform.set_fault(gate)
    platform.clear_fault(gate)
    replay_prefix = [entry.as_dict() for entry in platform.audit.replay()]
    journal_count = platform.audit.journal.count()
    platform.set_fault(gate)
    platform.clear_fault(gate)
    replay_full = [entry.as_dict() for entry in platform.audit.replay()]
    # 已有历史一条不改，新转换只追加。
    assert replay_full[: len(replay_prefix)] == replay_prefix
    assert platform.audit.journal.count() > journal_count
    kinds = [entry["kind"] for entry in replay_full]
    partition_kinds = [kind for kind in kinds if kind.startswith("partition_")]
    assert partition_kinds == [
        "partition_isolated",
        "partition_recovered",
        "partition_isolated",
        "partition_recovered",
    ]
    # 状态文档修订号单调递增，不回写旧状态。
    assert platform.partitions.get("ST1-P01").revision == 5


# ---- 并发放行 -----------------------------------------------------------


def test_concurrent_release_across_stations(platform):
    errors: list[Exception] = []

    def work(station_id: str) -> None:
        try:
            for _ in range(5):
                platform.batch_open(station_id)
                platform.sweep(station_id)
        except Exception as error:  # noqa: BLE001 - 汇总后统一断言
            errors.append(error)

    def toggle_fault() -> None:
        gate = _first_gate(platform, "ST2", "G01").id
        try:
            for _ in range(5):
                platform.set_fault(gate)
                platform.clear_fault(gate)
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    threads = [
        threading.Thread(target=work, args=(station_id,))
        for station_id in ("ST1", "ST2", "ST3")
    ]
    threads.append(threading.Thread(target=toggle_fault))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    # 并发结束后顺序语义仍然成立：各站都能按自己的分区放行。
    for station_id in ("ST1", "ST2", "ST3"):
        platform.sweep(station_id)
        gate = _first_gate(platform, station_id, "G01").id
        if platform.faults.is_active(gate):
            platform.clear_fault(gate)
        outcome = platform.batch_open(station_id)
        assert outcome["opened"]


# ---- 性能 ---------------------------------------------------------------


def test_batch_open_reads_gate_index_once(platform):
    calls = {"count": 0}
    original = platform.gates.list

    def counting():
        calls["count"] += 1
        return original()

    platform.gates.list = counting
    platform.batch_open("ST1")
    # 门清单一次取回按组建索引，不再按门区反复扫全站。
    assert calls["count"] == 1


def test_batch_open_scales_with_more_partitions(tmp_path, clock):
    config = replace(
        default_config(),
        station=StationSection(groups_per_station=8, doors_per_group=6),
    )
    platform = build_platform(config, clock=clock, storage_root=str(tmp_path / "state"))
    assert len(platform.partitions.list("ST1")) == 4
    platform.set_fault(_first_gate(platform, "ST1", "G01").id)
    outcome = platform.batch_open("ST1")
    assert outcome["skipped"] == {"G01": "partition_isolated", "G02": "partition_isolated"}
    assert len(outcome["opened"]) == 6
    assert len(outcome["domains"]) == 4


# ---- 配置 ---------------------------------------------------------------


def test_partition_config_roundtrip_and_validation():
    payload = config_to_mapping(default_config())
    assert payload["partition"]["failure_mode"] == "isolate"
    restored = config_from_mapping(payload)
    assert restored.partition.groups_per_partition == 2
    assert restored.partition.allow_merge is True
    bad = config_to_mapping(default_config())
    bad["partition"]["failure_mode"] = "bogus"
    with pytest.raises(ConfigError):
        config_from_mapping(bad)
    bad = config_to_mapping(default_config())
    bad["partition"]["groups_per_partition"] = 0
    with pytest.raises(ConfigError):
        config_from_mapping(bad)


# ---- 控制台 -------------------------------------------------------------


def test_console_partition_endpoints(platform):
    from psdline.console.api import Api

    api = Api(platform)
    status, payload = api.handle("GET", "/api/partitions", {}, {})
    assert status == 200
    assert payload["mode"] == "isolate"
    assert len(payload["domains"]) == 6
    status, _ = api.handle(
        "POST", "/api/partition/isolate", {}, {"partition_id": "ST1-P01", "reason": "maintenance"}
    )
    assert status == 200
    status, view = api.handle("GET", "/api/partitions/ST1", {}, {})
    assert status == 200
    states = {domain["key"]: domain["state"] for domain in view["domains"]}
    assert states["ST1-P01"] == ISOLATED
    assert states["ST1-P02"] == ACTIVE
    status, recovered = api.handle(
        "POST", "/api/partition/recover", {}, {"partition_id": "ST1-P01"}
    )
    assert status == 200 and recovered["ok"] is True
    status, merged = api.handle("POST", "/api/partition/merge", {}, {"station_id": "ST1"})
    assert status == 200 and merged["ok"] is True
    status, split = api.handle("POST", "/api/partition/split", {}, {"station_id": "ST1"})
    assert status == 200 and split["ok"] is True
    status, error = api.handle(
        "POST", "/api/partition/recover", {}, {"partition_id": "ST1-P99"}
    )
    assert status == 409
    assert error["component"] == "partition"


# ---- 联锁一致性 ---------------------------------------------------------


def test_depart_preserves_fault_lock(platform):
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01")
    gate = _first_gate(platform)
    platform.set_fault(gate.id)
    platform.depart("T01", "ST1")
    # 发车不能顺手放掉故障锁：故障没清，门必须保持锁定。
    assert platform.lockouts.is_locked(gate.id) is True
    assert platform.faults.is_active(gate.id)


def test_depart_releases_transient_lock_on_failure(platform):
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01", clear=False)
    gate = platform._primary_gate("ST1")
    outcome = platform.depart("T01", "ST1")
    assert outcome["ok"] is False
    # 发车失败时瞬时锁必须解开，不能留在现场。
    assert platform.lockouts.is_locked(gate.id) is False


# ---- 登记表直测 ---------------------------------------------------------


def test_registry_ensure_is_idempotent(tmp_path):
    registry = PartitionRegistry(JsonStore(str(tmp_path)))
    first = registry.ensure("ST1", ["G01", "G02", "G03"], 2)
    assert [item.id for item in first] == ["ST1-P01", "ST1-P02"]
    again = registry.ensure("ST1", ["G01", "G02", "G03"], 2)
    assert [item.id for item in again] == ["ST1-P01", "ST1-P02"]
    # 新增门区只补建新区，已有指派原样保留。
    registry.isolate("ST1-P01", "fault", at=1.0)
    extended = registry.ensure("ST1", ["G01", "G02", "G03", "G04"], 2)
    assert [item.id for item in extended] == ["ST1-P01", "ST1-P02", "ST1-P03"]
    assert extended[0].state == ISOLATED
    assert extended[2].groups == ("G04",)
