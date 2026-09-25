"""A calc whose parse or write fails OUTSIDE the parser's own error handling — offline.

Seen live (Materials Cloud, 2026-09-25): a 4-atom vasprun with only 2 force rows (inside a code
tarball) parsed "fine" — ``REF_forces`` is assigned straight into ``atoms.arrays`` — then failed in
the extxyz writer, and the parallel loop logged "parse worker raised unexpectedly" with no calc_id
and no rejection, so the calc vanished from the audit trail. Now ``_frame`` rejects the malformed
forces at parse time, and anything that still escapes is recorded as a ``parse_error`` rejection
against its calc (both loops) — except storage errors, which stop the parse.
"""

from __future__ import annotations

import errno
import json
from pathlib import Path

import numpy as np
import pytest
from ase import Atoms

from zenodo_harvest import parse as parse_mod
from zenodo_harvest.dataset_ops import verify_dataset


def _manifest(tmp_path: Path, names: list[str]) -> tuple[Path, Path]:
    raw = tmp_path / "raw"
    units = []
    for name in names:
        d = raw / "r" / "extracted" / name
        d.mkdir(parents=True)
        (d / "vasprun.xml").write_bytes(b"<modeling/>")
        units.append({"vasprun": f"r/extracted/{name}/vasprun.xml", "dir": f"r/extracted/{name}"})
    fetched = tmp_path / "fetched.jsonl"
    fetched.write_text(json.dumps({"recid": "r", "local_dir": "r", "calc_units": units,
                                   "provenance": {"source": "test", "record_id": "r"}}) + "\n")
    return fetched, raw


def _result(calc_id: str, force_rows: int = 2) -> tuple[list[Atoms], dict]:
    atoms = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]], cell=[5, 5, 5], pbc=True)
    atoms.info.update({"calc_id": calc_id, "frame_id": f"{calc_id}#0", "ionic_step": 0,
                       "REF_energy": -1.0, "source": "test"})
    atoms.arrays["REF_forces"] = np.zeros((force_rows, 3))   # != 2 rows -> the writer raises
    return [atoms], {"calc_id": calc_id, "frame_ids": [f"{calc_id}#0"], "parser": "stub"}


def _rejections(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.is_file() else []


@pytest.mark.parametrize("workers", [1, 4])
def test_escaped_write_error_is_rejected_against_its_calc(tmp_path, monkeypatch, workers):
    fetched, raw = _manifest(tmp_path, ["a", "bad", "b", "c"])

    def fake_parse_one(unit, base_meta, availability, rej, cap, timeout, calc_id):
        # 3 rows for 2 atoms cannot broadcast (1 row WOULD — numpy would silently copy it onto
        # every atom, which is why _frame checks the shape up front)
        return _result(calc_id, force_rows=3 if calc_id.endswith(":bad/vasprun.xml") else 2)

    monkeypatch.setattr(parse_mod, "_parse_one", fake_parse_one)
    ds, rej_path = tmp_path / "ds", tmp_path / "rej.jsonl"
    stats = parse_mod.parse(fetched, dataset_dir=ds, raw_dir=raw, rejections_path=rej_path,
                            parse_workers=workers)
    assert stats["calcs_parsed"] == 3 and stats["frames"] == 3   # the run carried on
    rows = _rejections(rej_path)
    assert len(rows) == 1 and rows[0]["reason"] == "parse_error"
    assert rows[0]["id"] == "test:r:bad/vasprun.xml"
    assert "could not broadcast" in rows[0]["detail"]
    report = verify_dataset(ds)
    assert report["ok"] and report["integrity"]["n_orphans_on_disk"] == 0   # nothing half-written


@pytest.mark.parametrize("workers", [1, 4])
def test_storage_error_stops_the_parse(tmp_path, monkeypatch, workers):
    fetched, raw = _manifest(tmp_path, ["a", "b"])

    def fake_parse_one(unit, base_meta, availability, rej, cap, timeout, calc_id):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(parse_mod, "_parse_one", fake_parse_one)
    with pytest.raises(OSError):
        parse_mod.parse(fetched, dataset_dir=tmp_path / "ds", raw_dir=raw,
                        rejections_path=tmp_path / "rej.jsonl", parse_workers=workers)
    assert _rejections(tmp_path / "rej.jsonl") == []   # not blamed on (and not retried as) a calc


def test_frame_rejects_forces_that_do_not_match_the_atoms():
    pytest.importorskip("pymatgen")
    from pymatgen.core import Lattice, Structure
    st = Structure(Lattice.cubic(4.0), ["Si"] * 4,
                   [[0, 0, 0], [0.5, 0.5, 0], [0.5, 0, 0.5], [0, 0.5, 0.5]])
    kw = dict(calc_id="c", frame_id="c#0", ionic_step=0, electronic_converged=True, scf_dE=None)
    with pytest.raises(ValueError, match="forces shape"):
        parse_mod._frame(st, -1.0, [[0.0, 0.0, 0.0]] * 2, **kw)
    ok = parse_mod._frame(st, -1.0, [[0.0, 0.0, 0.1]] * 4, **kw)
    assert ok.arrays["REF_forces"].shape == (4, 3)
