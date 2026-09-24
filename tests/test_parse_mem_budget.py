"""RAM-aware admission for concurrent parses (``parse(parse_mem_budget=…)``) — offline.

The parse itself is stubbed (no pymatgen): what is under test is the scheduling contract — each
parse reserves its own peak-RSS estimate, FIFO, so small calcs overlap while a big primary runs
alone, the reservations never exceed the budget, and the default (no budget) is unchanged.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from zenodo_harvest import parse as parse_mod


def test_mem_budget_is_fifo_and_clamps():
    mem = parse_mod._MemBudget(10)
    assert mem.acquire(100) == 10                         # clamped: runs alone
    order: list[str] = []

    def take(name: str, n: int) -> None:
        got = mem.acquire(n)
        order.append(name)
        mem.release(got)

    b = threading.Thread(target=take, args=("big", 6))
    b.start()
    time.sleep(0.05)                                      # "big" is queued first ...
    s = threading.Thread(target=take, args=("small", 1))
    s.start()
    time.sleep(0.05)
    assert order == []                                    # ... and "small" may not overtake it
    mem.release(10)
    b.join(2)
    s.join(2)
    assert order == ["big", "small"] and mem.waits == 2 and mem.peak_reserved == 10


def test_estimate_uses_largest_parseable_primary(tmp_path):
    big, small = tmp_path / "vasprun.xml", tmp_path / "OUTCAR"
    with big.open("wb") as fh:
        fh.truncate(5_000_000)                            # sparse: size without the bytes
    small.write_bytes(b"x" * 1000)
    unit = {"vasprun": str(big), "outcar": str(small)}
    base = parse_mod.PARSE_RSS_BASE_BYTES
    assert parse_mod._estimated_parse_rss(unit, 0, 2.0) == base + 10_000_000
    # a primary over the cap is refused before parsing, so only the OUTCAR is ever parsed
    assert parse_mod._estimated_parse_rss(unit, 1_000_000, 2.0) == base + 2000


def _manifest(tmp_path: Path, sizes: dict[str, int]) -> tuple[Path, Path]:
    raw = tmp_path / "raw"
    units = []
    for name, size in sizes.items():
        d = raw / "r" / "extracted" / name
        d.mkdir(parents=True)
        with (d / "vasprun.xml").open("wb") as fh:
            fh.truncate(size)
        units.append({"vasprun": f"r/extracted/{name}/vasprun.xml", "dir": f"r/extracted/{name}"})
    fetched = tmp_path / "fetched.jsonl"
    fetched.write_text(json.dumps({"recid": "r", "local_dir": "r", "calc_units": units,
                                   "provenance": {"source": "test", "record_id": "r"}}) + "\n")
    return fetched, raw


@pytest.mark.parametrize("budget", [0, 5_000_000])
def test_parse_admission_runs_big_primary_alone(tmp_path, monkeypatch, budget):
    monkeypatch.setattr(parse_mod, "PARSE_RSS_BASE_BYTES", 0)
    sizes = {"big": 5_000_000, **{f"s{i}": 1000 for i in range(8)}}
    fetched, raw = _manifest(tmp_path, sizes)
    lock = threading.Lock()
    running: set[str] = set()
    overlaps: dict[str, set[str]] = {n: set() for n in sizes}
    peak = [0]

    def fake_parse_one(unit, base_meta, availability, rej, cap, timeout, calc_id):
        name = Path(unit["vasprun"]).parent.name
        with lock:
            for other in running:
                overlaps[name].add(other)
                overlaps[other].add(name)
            running.add(name)
            peak[0] = max(peak[0], len(running))
        time.sleep(0.03)
        with lock:
            running.discard(name)
        return None                                       # nothing to write

    monkeypatch.setattr(parse_mod, "_parse_one", fake_parse_one)
    stats = parse_mod.parse(fetched, dataset_dir=tmp_path / "ds", raw_dir=raw,
                            rejections_path=tmp_path / "rej.jsonl", parse_workers=4,
                            parse_mem_budget=budget, parse_rss_ratio=1.0)
    assert stats["calc_units"] == 9 and peak[0] > 1       # small calcs DO run concurrently
    if budget:
        assert overlaps["big"] == set()                   # the 5 MB primary ran alone
        assert stats["mem_budget_peak_GiB"] <= budget / 2**30 + 1e-9
    else:
        assert "mem_budget_peak_GiB" not in stats         # default: no admission control
