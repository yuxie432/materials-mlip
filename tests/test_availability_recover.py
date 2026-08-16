"""Offline tests for the per-calc `availability` recovery (``zenodo_harvest.availability_recover``).

Builds a synthetic dataset — a real gzipped shard + a ``metadata.jsonl`` carrying the OLD
(record-level, filename-only) availability — with a re-fetched ``fetched.jsonl`` that has the new
per-calc ``calc_availability`` and staged vasprun/OUTCAR files, and checks that
:func:`refresh_availability_metadata`:

* fixes the OVER-count — a CHGCAR beside calc c1 no longer flags sibling calc c2;
* fixes the UNDER-count — DOS/eigenvalues embedded in c1's vasprun.xml (no DOSCAR/EIGENVAL file)
  are recovered via the embedded probe;
* re-derives spin_density/magnetization from each record's own spin_polarized/site_magmoms;
* leaves ``calc_id``/``frame_ids``/``shards``/``calc_parameters`` (and the shards) byte-identical;
* is resume-safe: a calc whose primary is no longer staged (purged) is SKIPPED, not clobbered.

Needs ase (the shard writer + importing parse). No pymatgen — the embedded probe is a byte scan.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

pytest.importorskip("ase")
from ase import Atoms  # noqa: E402

from zenodo_harvest.availability_recover import (  # noqa: E402
    build_availability_keeplist,
    refresh_availability_metadata,
)
from zenodo_harvest.store import ShardedExtxyzWriter  # noqa: E402

# c1's vasprun EMBEDS dos + eigenvalues (no DOSCAR/EIGENVAL file); c2's embeds neither.
_VASPRUN_WITH_DOS = "<modeling><calculation><eigenvalues></eigenvalues><dos></dos></calculation></modeling>"
_VASPRUN_PLAIN = "<modeling><calculation><structure></structure></calculation></modeling>"
_ALL7 = ("charge_density", "wavefunction", "dos", "eigenvalues", "projected",
         "local_potential", "elf")


def _heavy(**on: bool) -> dict:
    """A full 7-key heavy-availability dict with the named flags True (rest False)."""
    return {k: bool(on.get(k, False)) for k in _ALL7}


def _write_shard(ds: Path, frame_ids: list[str]) -> None:
    with ShardedExtxyzWriter(ds) as xyz:
        for fid in frame_ids:
            a = Atoms("H", positions=[[0, 0, 0]], cell=[10, 10, 10], pbc=True)
            a.info["frame_id"] = fid
            a.info["calc_id"] = fid.split("#")[0]
            xyz.write(a)
        xyz.flush()


def _old_avail(**on: bool) -> dict:
    """An OLD availability block (7 heavy + the 2 spin-derived keys), as the first harvest wrote."""
    a = _heavy(**on)
    a["spin_density"] = bool(on.get("spin_density", False))
    a["magnetization"] = bool(on.get("magnetization", False))
    return a


def _build(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    raw = tmp_path / "raw"
    ds = tmp_path / "ds"
    ds.mkdir(parents=True)

    # R1: two vasprun calcs. c1 has a CHGCAR beside it + embedded dos/eigen; c2 has neither.
    (raw / "R1" / "extracted" / "c1").mkdir(parents=True)
    (raw / "R1" / "extracted" / "c1" / "vasprun.xml").write_text(_VASPRUN_WITH_DOS)
    (raw / "R1" / "extracted" / "c2").mkdir(parents=True)
    (raw / "R1" / "extracted" / "c2" / "vasprun.xml").write_text(_VASPRUN_PLAIN)
    # R2: an OUTCAR-only calc with a DOSCAR beside it (filename flag; no embedded probe).
    (raw / "R2" / "extracted" / "c").mkdir(parents=True)
    (raw / "R2" / "extracted" / "c" / "OUTCAR").write_text(" vasp.6\n")

    frame_ids = ["zenodo:R1:c1/vasprun.xml#0", "zenodo:R1:c2/vasprun.xml#0",
                 "zenodo:R2:c/OUTCAR#0"]
    _write_shard(ds, frame_ids)

    # OLD metadata: the over-counted, under-counted availability the first harvest produced —
    # CHGCAR OR'd onto BOTH R1 calcs; embedded dos/eigen missed (False) everywhere.
    recs = [
        {"calc_id": "zenodo:R1:c1/vasprun.xml", "parser": "pymatgen.Vasprun",
         "calc_parameters": {"spin_polarized": True}, "site_magmoms_present": False,
         "availability": _old_avail(charge_density=True, spin_density=True, magnetization=True),
         "frame_ids": ["zenodo:R1:c1/vasprun.xml#0"], "shards": ["shard-00000.extxyz.gz"],
         "provenance": {"source": "zenodo", "record_id": "R1"}},
        {"calc_id": "zenodo:R1:c2/vasprun.xml", "parser": "pymatgen.Vasprun",
         "calc_parameters": {"spin_polarized": False}, "site_magmoms_present": False,
         "availability": _old_avail(charge_density=True),   # OVER-count: c2 has no CHGCAR
         "frame_ids": ["zenodo:R1:c2/vasprun.xml#0"], "shards": ["shard-00000.extxyz.gz"],
         "provenance": {"source": "zenodo", "record_id": "R1"}},
        {"calc_id": "zenodo:R2:c/OUTCAR", "parser": "ase.OUTCAR",
         "calc_parameters": {"spin_polarized": False}, "site_magmoms_present": False,
         "availability": _old_avail(),
         "frame_ids": ["zenodo:R2:c/OUTCAR#0"], "shards": ["shard-00000.extxyz.gz"],
         "provenance": {"source": "zenodo", "record_id": "R2"}},
    ]
    (ds / "metadata.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))

    keep = tmp_path / "keep.jsonl"
    keep.write_text("".join(json.dumps({"recid": r, "bytes_total": 1000, "files": []}) + "\n"
                            for r in ("R1", "R2")))

    # A re-fetched manifest (new code): per-calc calc_availability aligned with calc_units.
    fetched = tmp_path / "fetched.jsonl"
    fentries = [
        {"recid": "R1", "provenance": {"source": "zenodo", "record_id": "R1"}, "local_dir": "R1",
         "calc_units": [{"dir": "R1/extracted/c1", "vasprun": "R1/extracted/c1/vasprun.xml"},
                        {"dir": "R1/extracted/c2", "vasprun": "R1/extracted/c2/vasprun.xml"}],
         "availability": _heavy(charge_density=True),      # record union (unused by refresh)
         "calc_availability": [_heavy(charge_density=True), _heavy()]},   # c1 has CHGCAR, c2 none
        {"recid": "R2", "provenance": {"source": "zenodo", "record_id": "R2"}, "local_dir": "R2",
         "calc_units": [{"dir": "R2/extracted/c", "outcar": "R2/extracted/c/OUTCAR"}],
         "availability": _heavy(dos=True),
         "calc_availability": [_heavy(dos=True)]},          # DOSCAR file -> dos flag
    ]
    fetched.write_text("".join(json.dumps(e) + "\n" for e in fentries))
    return ds, raw, keep, fetched


def _load(ds: Path) -> dict:
    return {r["calc_id"]: r for r in
            (json.loads(x) for x in (ds / "metadata.jsonl").read_text().splitlines())}


def _shard_md5(ds: Path) -> dict:
    return {p.name: hashlib.md5(p.read_bytes()).hexdigest()
            for p in sorted(ds.glob("shard-*.extxyz.gz"))}


def test_keeplist_selects_every_record_with_a_calc(tmp_path):
    ds, _raw, keep, _ = _build(tmp_path)
    summ = build_availability_keeplist(ds, keep, tmp_path / "ak.jsonl")
    assert summ["ok"] and summ["records_targeted"] == 2 and summ["records_written"] == 2
    recids = {json.loads(x)["recid"] for x in (tmp_path / "ak.jsonl").read_text().splitlines()}
    assert recids == {"R1", "R2"}                # every record, not a parser subset


def test_keeplist_reports_records_missing_from_keep(tmp_path):
    ds, _raw, keep, _ = _build(tmp_path)
    keep.write_text(json.dumps({"recid": "R1", "files": []}) + "\n")   # drop R2's URLs
    summ = build_availability_keeplist(ds, keep, tmp_path / "ak.jsonl")
    assert summ["records_written"] == 1 and summ["missing_from_keep"] == ["R2"]


def test_refresh_fixes_overcount_and_undercount_and_spin(tmp_path):
    ds, raw, _keep, fetched = _build(tmp_path)
    before_keys = {cid: (r["parser"], tuple(r["frame_ids"]), tuple(r["shards"]),
                         json.dumps(r["calc_parameters"], sort_keys=True))
                   for cid, r in _load(ds).items()}
    before_shards = _shard_md5(ds)

    summ = refresh_availability_metadata(ds, fetched, raw)
    assert summ["ok"] and summ["records_refreshed"] == 3 and summ["calcs_present_this_pass"] == 3

    recs = _load(ds)
    c1 = recs["zenodo:R1:c1/vasprun.xml"]["availability"]
    c2 = recs["zenodo:R1:c2/vasprun.xml"]["availability"]
    r2 = recs["zenodo:R2:c/OUTCAR"]["availability"]
    # c1: CHGCAR (its own dir) + embedded dos/eigen recovered; spin-polarised -> spin flags on
    assert c1["charge_density"] is True and c1["dos"] is True and c1["eigenvalues"] is True
    assert c1["spin_density"] is True and c1["magnetization"] is True
    # c2: OVER-count fixed — no CHGCAR here, and its vasprun embeds nothing
    assert c2["charge_density"] is False and c2["dos"] is False and c2["eigenvalues"] is False
    assert c2["spin_density"] is False and c2["magnetization"] is False
    # R2 (OUTCAR-only): dos from the DOSCAR filename flag; no embedded probe
    assert r2["dos"] is True and r2["eigenvalues"] is False
    # every other field + the shards are byte-identical (metadata-only, no shard touched)
    assert {cid: (r["parser"], tuple(r["frame_ids"]), tuple(r["shards"]),
                  json.dumps(r["calc_parameters"], sort_keys=True))
            for cid, r in recs.items()} == before_keys
    assert _shard_md5(ds) == before_shards


def test_refresh_skips_purged_calcs_resume_safe(tmp_path):
    # A calc whose primary was purged by an earlier batch must be SKIPPED, never recomputed from
    # the missing file (which would drop its embedded dos/eigen). Delete c1's staged vasprun and
    # confirm its (correct) prior availability is left untouched while c2/R2 still refresh.
    ds, raw, _keep, fetched = _build(tmp_path)
    # Seed c1 with an ALREADY-CORRECT availability (as if a prior batch refreshed it), then purge.
    recs = _load(ds)
    recs["zenodo:R1:c1/vasprun.xml"]["availability"] = _old_avail(
        charge_density=True, dos=True, eigenvalues=True, spin_density=True, magnetization=True)
    (ds / "metadata.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs.values()))
    (raw / "R1" / "extracted" / "c1" / "vasprun.xml").unlink()      # simulate purge

    summ = refresh_availability_metadata(ds, fetched, raw)
    assert summ["calcs_skipped_absent"] == 1        # c1 skipped (file gone)
    assert summ["calcs_present_this_pass"] == 2     # c2 + R2 still refreshed
    c1 = _load(ds)["zenodo:R1:c1/vasprun.xml"]["availability"]
    assert c1["dos"] is True and c1["eigenvalues"] is True and c1["charge_density"] is True


def test_refresh_idempotent_dry_run_and_backup(tmp_path):
    ds, raw, _keep, fetched = _build(tmp_path)
    pristine = (ds / "metadata.jsonl").read_text()
    summ = refresh_availability_metadata(ds, fetched, raw, dry_run=True)
    assert summ["records_refreshed"] == 3
    assert (ds / "metadata.jsonl").read_text() == pristine            # dry-run writes nothing
    assert not (ds / "metadata.jsonl.bak.pre_availability_refresh").exists()

    refresh_availability_metadata(ds, fetched, raw)
    bak = ds / "metadata.jsonl.bak.pre_availability_refresh"
    assert bak.exists() and bak.read_text() == pristine               # one-time pre-refresh backup
    once = (ds / "metadata.jsonl").read_text()
    refresh_availability_metadata(ds, fetched, raw)                    # idempotent
    assert (ds / "metadata.jsonl").read_text() == once and bak.read_text() == pristine


def test_verify_still_passes_after_refresh(tmp_path):
    from zenodo_harvest.dataset_ops import verify_dataset   # reads shards via ase; no pymatgen
    ds, raw, _keep, fetched = _build(tmp_path)
    refresh_availability_metadata(ds, fetched, raw)
    out = verify_dataset(ds)
    assert out["ok"] is True
    assert out["integrity"]["n_frames_metadata"] == out["integrity"]["n_frames_on_disk"] == 3
