"""控制分区：隔离、独立限流、失败补偿、崩溃恢复与审计一致。"""

from __future__ import annotations

import threading

import pytest

from psdline.app import build_platform
from psdline.config import config_from_mapping, config_to_mapping, default_config
from psdline.partition import (
    ACTIVE,
    DEGRADED,
    Partition,
    PartitionError,
    partition_quota_name,
)


def _multi_partition_config(groups_per_partition: int = 2, tmp_path=None):
    mapping = config_to_mapping(default_config())
    mapping["partition"] = {"groups_per_partition": groups_per_partition}
    return config_from_mapping(mapping)


def _platform(tmp_path, width: int = 2):
    cfg = _multi_partition_config(width)
    return build_platform(cfg, storage_root=str(tmp_path / "state"))


def test_partition_dataclass_validates():
    p = Partition(id="ST1-P01", station_id="ST1", groups=("G01", "G02"))
    assert p.active and not p.degraded
    assert p.as_dict()["groups"] == ["G01", "G02"]
    with pytest.raises(PartitionError):
        Partition(id="", station_id="ST1", groups=("G01",))
    with pytest.raises(PartitionError):
        Partition(id="x", station_id="ST1", groups=())
    with pytest.raises(PartitionError):
        Partition(id="x", station_id="ST1", groups=("G01", "G01"))


def test_default_config_provisions_one_partition_per_station(platform):
    by_station = platform.partitions.by_station("ST1")
    assert [p.id for p in by_station] == ["ST1-P01"]
    assert by_station[0].groups == ("G01", "G02", "G03", "G04")
    assert platform.partitions.summary()["count"] == 3


def test_multi_partition_provisioning(tmp_path):
    p = _platform(tmp_path, width=2)
    p.start()
    ids = [(x.id, x.groups) for x in p.partitions.by_station("ST1")]
    assert ids == [("ST1-P01", ("G01", "G02")), ("ST1-P02", ("G03", "G04"))]


def test_isolate_locks_only_own_partition(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", reason="fault")
    p01 = p.partitions.require("ST1-P01")
    p02 = p.partitions.require("ST1-P02")
    assert p01.degraded and p02.active
    # P01 的 12 樘门全锁，P02 一樘不锁
    assert p.partitions.summary()["partitions"][0]["locked_count"] == 12
    assert all(not g.locked for g in p.gates.by_station("ST1") if g.group in ("G03", "G04"))


def test_arrive_skips_degraded_partition_but_opens_others(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", reason="fault")
    report = p.arrive("T01", "ST1", 0)
    assert report["ok"] is True
    assert report["opened_groups"] == ["G03", "G04"]
    assert report["degraded_partitions"] == ["ST1-P01"]
    open_groups = {g.group for g in p.gates.by_station("ST1") if g.open}
    assert open_groups == {"G03", "G04"}


def test_other_station_unaffected_by_partition_fault(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", reason="fault")
    # ST2 停稳位在 24m，完全不受 ST1 故障影响
    report = p.arrive("T02", "ST2", 24)
    assert report["ok"] is True
    assert report["opened_groups"] == ["G01", "G02", "G03", "G04"]
    assert report["degraded_partitions"] == []


def test_all_partitions_degraded_rejects(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", "x")
    p.isolate_partition("ST1-P02", "x")
    report = p.arrive("T01", "ST1", 0)
    assert report["ok"] is False
    assert "全部" in report["violations"][0]


def test_per_partition_quota_independent(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.set_partition_quota("ST1-P01", "arrival", 1)
    assert p.arrive("T1", "ST1", 0)["ok"] is True
    rejected = p.arrive("T2", "ST1", 0)
    assert rejected["ok"] is False
    assert "ST1-P01" in rejected["violations"][0]
    # 被拒后 P02 与整站配额都不应增加
    assert p.quotas.get(partition_quota_name("ST1-P02", "arrival")).used == 1
    assert p.quotas.get("arrival").used == 1
    p.set_partition_quota("ST1-P01", "arrival", 1000)
    assert p.arrive("T3", "ST1", 0)["ok"] is True


def test_depart_refunds_quota_on_interlock_failure(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.quotas.set_limit("depart", 10)
    p.arrive("T1", "ST1", 0)
    p.check_gap("T1", clear=False)
    rejected = p.depart("T1", "ST1")
    assert rejected["ok"] is False
    assert p.quotas.get("depart").used == 0
    assert p.quotas.get(partition_quota_name("ST1-P01", "depart")).used == 0
    assert p.quotas.get(partition_quota_name("ST1-P02", "depart")).used == 0
    p.check_gap("T1", True)
    ok = p.depart("T1", "ST1")
    assert ok["ok"] is True
    assert p.quotas.get("depart").used == 1


def test_recover_requires_closed_gates(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", "x")
    # 模拟本区有门开着（人工/异常），不能并回
    gate = next(g for g in p.gates.by_station("ST1") if g.group == "G01")
    p.gates.set_open(gate.id, True)
    rec = p.recover_partition("ST1-P01")
    assert rec["ok"] is False and rec["violations"] == ["gate_open"]
    p.gates.set_open(gate.id, False)
    rec = p.recover_partition("ST1-P01")
    assert rec["ok"] is True
    assert p.partitions.require("ST1-P01").active


def test_batch_open_labels_partition_degraded(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", "x")
    out = p.batch_open("ST1")
    assert out["opened"] == ["G03", "G04"]
    assert out["skipped"] == {"G01": "partition_degraded", "G02": "partition_degraded"}
    assert out["partitions"]["ST1-P01"]["phase"] == DEGRADED


def test_concurrent_arrivals_other_partitions_still_release(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", "fault")
    results: list[bool] = []
    threads = [
        threading.Thread(target=lambda i=i: results.append(p.arrive(f"T{i}", "ST1", 0)["ok"]))
        for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(results) and len(results) == 8
    healthy_open = [
        g for g in p.gates.by_station("ST1") if g.group in ("G03", "G04") and g.open
    ]
    assert len(healthy_open) == 12


def test_partition_phase_survives_restart(tmp_path):
    cfg = _multi_partition_config(2)
    p = build_platform(cfg, storage_root=str(tmp_path / "state"))
    p.start()
    p.isolate_partition("ST1-P01", "power_loss")
    p2 = build_platform(cfg, storage_root=str(tmp_path / "state"))
    phases = {x.id: x.phase for x in p2.partitions.by_station("ST1")}
    assert phases == {"ST1-P01": DEGRADED, "ST1-P02": ACTIVE}
    # 重启后退出分区的门仍然锁着
    assert all(
        g.locked for g in p2.gates.by_station("ST1") if g.group in ("G01", "G02")
    )
    # 手动并回后恢复
    assert p2.recover_partition("ST1-P01")["ok"] is True


def test_partition_lifecycle_is_audited(tmp_path):
    p = _platform(tmp_path)
    p.start()
    p.isolate_partition("ST1-P01", "fault")
    p.recover_partition("ST1-P01")
    kinds = [e.kind for e in p.audit.list()]
    assert "partition_isolate" in kinds
    assert "partition_recover" in kinds
    # 到站跳过退出分区也有审计
    p.isolate_partition("ST1-P02", "fault")
    p.arrive("T9", "ST1", 0)
    assert any(e.kind == "partition_skipped" for e in p.audit.list())


def test_sweep_isolates_bad_partition_and_sweeps_rest(tmp_path):
    p = _platform(tmp_path)
    p.start()
    # 打开门，正常清扫应关闭；让 P01 的门关闭写入抛错模拟坏分区
    p.batch_open("ST1")  # 全开
    original = p.gates.set_open

    def flaky(psd_id, open_state):
        gate = p.gates.get(psd_id)
        if gate is not None and gate.group in ("G01", "G02") and not open_state:
            from psdline.errors import StorageError

            raise StorageError("device offline", component="psd")
        return original(psd_id, open_state)

    p.gates.set_open = flaky
    result = p.sweep("ST1")
    assert result["ok"] is False
    assert "ST1-P01" in result["failed_partitions"]
    assert p.partitions.require("ST1-P01").degraded
    # P02 仍被清扫关闭
    p.gates.set_open = original
    assert all(not g.open for g in p.gates.by_station("ST1") if g.group in ("G03", "G04"))
