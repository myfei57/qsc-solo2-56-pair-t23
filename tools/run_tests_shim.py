"""最小 pytest 垫片：仅用于本无 pip 环境跑既有测试。

支持：fixture（tmp_path/clock/platform/online 及 tmp_path 注入）、
``pytest.raises``、测试函数发现。不追求与 pytest 完全一致，只够执行本仓库用例。
"""
from __future__ import annotations

import importlib.util
import inspect
import pathlib
import sys
import tempfile
import traceback
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class _Raises:
    def __init__(self, exc):
        self.exc = exc
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, etype, evalue, tb):
        if evalue is None:
            raise AssertionError(f"expected {self.exc.__name__}, nothing raised")
        if not issubclass(etype, self.exc):
            return False
        self.value = evalue
        return True


def raises(exc):
    return _Raises(exc)


def _make_fixtures(base: pathlib.Path, wanted: set[str]):
    from psdline.app import build_platform
    from psdline.clock import ManualClock
    from psdline.config import default_config

    fix: dict = {"tmp_path": base}
    if {"clock", "platform", "online"} & wanted:
        clock = ManualClock()
        platform = build_platform(
            default_config(), clock=clock, storage_root=str(base / "state")
        )
        fix["clock"] = clock
        fix["platform"] = platform
    if "online" in wanted:
        platform.start()
        platform.arrive("T01", "ST1", 0)
        platform.check_gap("T01")
        fix["online"] = platform
    return fix


def main():
    pytest = types.ModuleType("pytest")
    pytest.raises = raises
    pytest.fixture = lambda *a, **k: (lambda f: f)
    sys.modules["pytest"] = pytest

    testdir = ROOT / "tests"
    files = sorted(testdir.glob("test_*.py"))
    total = passed = failed = 0
    failures = []
    for path in files:
        name = "tests." + path.stem
        spec = importlib.util.spec_from_file_location(name, path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception:
            print(f"IMPORT ERROR {path.name}")
            traceback.print_exc()
            failed += 1
            total += 1
            continue
        for fname, fn in vars(mod).items():
            if not (inspect.isfunction(fn) and fname.startswith("test_")):
                continue
            sig = inspect.signature(fn)
            kwargs = {}
            if sig.parameters:
                base = pathlib.Path(tempfile.mkdtemp())
                wanted = set(sig.parameters)
                fix = _make_fixtures(base, wanted)
                for pname in sig.parameters:
                    kwargs[pname] = fix[pname]
            total += 1
            try:
                fn(**kwargs)
                passed += 1
            except Exception:
                failed += 1
                failures.append((f"{path.name}::{fname}"))
                print(f"FAIL {path.name}::{fname}")
                traceback.print_exc()
    print(f"\n{passed} passed, {failed} failed, {total} total")
    if failures:
        print("failures:", failures)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
