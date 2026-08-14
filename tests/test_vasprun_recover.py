"""Offline-ish tests for the vasprun `parameters` recovery (``zenodo_harvest.vasprun_recover``).

Builds a synthetic dataset — a real gzipped shard + a ``metadata.jsonl`` with one vasprun
record that LACKS a ``parameters`` block (like the original Zenodo harvest) and one OUTCAR
record that must not be touched — stages a minimal real vasprun.xml in a ``raw`` tree, and
checks that :func:`refresh_vasprun_metadata` adds the authoritative ``parameters`` block to the
vasprun record only, drops its stale ``resolved`` cache, and leaves ``run_type``/``incar``/
``calc_id``/``frame_ids``/``shards`` (and the OUTCAR record) byte-identical.

Needs ase (shard writer) + lxml + pymatgen (the header parse); skipped if pymatgen/lxml absent.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from ase import Atoms

from zenodo_harvest.store import ShardedExtxyzWriter
from zenodo_harvest.vasprun_recover import build_vasprun_keeplist, refresh_vasprun_metadata
from tests.test_vasprun_params import MINIMAL_VASPRUN


def _pmg_or_skip():
    pytest.importorskip("lxml")
    pytest.importorskip("pymatgen")


def _write_shard(ds: Path, frame_ids: list[str]) -> None:
    with ShardedExtxyzWriter(ds) as xyz:
        for fid in frame_ids:
            a = Atoms("H", positions=[[0, 0, 0]], cell=[10, 10, 10], pbc=True)
            a.info["frame_id"] = fid
            a.info["calc_id"] = fid.split("#")[0]
            xyz.write(a)
        xyz.flush()


def _vasprun_cp_no_params() -> dict:
    """A pre-recovery vasprun calc_parameters: rich EXCEPT no `parameters` block, plus a stale
    resolved cache (as the enrich step would have left it)."""
    return {"code": "vasp", "run_type": "PBE+vdW-TS", "functional": "PBE",
            "encut": 520.0, "incar": {"ENCUT": 520.0, "GGA": "PE"},
            "kpoints": {"num_kpts": 10}, "potcar_set_hash": "abc123",
            "resolved": {"incar_complete": True, "effective": {"ISIF": None}, "sources": {}}}


def _build(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    raw = tmp_path / "raw"
    ds = tmp_path / "ds"
    ds.mkdir(parents=True)
    # stage a real vasprun.xml for record V at a calc-id-consistent path
    vdir = raw / "V" / "extracted" / "vr"
    vdir.mkdir(parents=True)
    (vdir / "vasprun.xml").write_text(MINIMAL_VASPRUN)
    # stage an OUTCAR for record O (must NOT be refreshed by the vasprun path)
    odir = raw / "O" / "extracted" / "c"
    odir.mkdir(parents=True)
    (odir / "OUTCAR").write_text(" vasp.6\n INCAR:\n POTCAR: PAW_PBE O\n")

    frame_ids = ["zenodo:V:vr/vasprun.xml#0", "zenodo:O:c/OUTCAR#0"]
    _write_shard(ds, frame_ids)
    recs = [
        {"calc_id": "zenodo:V:vr/vasprun.xml", "parser": "pymatgen.Vasprun",
         "calc_parameters": _vasprun_cp_no_params(),
         "frame_ids": ["zenodo:V:vr/vasprun.xml#0"], "shards": ["shard-00000.extxyz.gz"],
         "provenance": {"source": "zenodo", "record_id": "V"}},
        {"calc_id": "zenodo:O:c/OUTCAR", "parser": "ase.OUTCAR",
         "calc_parameters": {"run_type": "GGA", "functional": "GGA", "parsed_from": "outcar_header",
                             "incar": {}, "parameters": {"GGA": "--"}},
         "frame_ids": ["zenodo:O:c/OUTCAR#0"], "shards": ["shard-00000.extxyz.gz"],
         "provenance": {"source": "zenodo", "record_id": "O"}},
    ]
    (ds / "metadata.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))

    keep = tmp_path / "keep.jsonl"
    keep.write_text("".join(json.dumps({"recid": r, "bytes_total": 1000, "files": []}) + "\n"
                            for r in ("V", "O")))
    fetched = tmp_path / "fetched.jsonl"
    fentries = [
        {"recid": "V", "provenance": {"source": "zenodo", "record_id": "V"},
         "local_dir": "V", "calc_units": [{"dir": "V/extracted/vr",
                                           "vasprun": "V/extracted/vr/vasprun.xml"}]},
        {"recid": "O", "provenance": {"source": "zenodo", "record_id": "O"},
         "local_dir": "O", "calc_units": [{"dir": "O/extracted/c",
                                           "outcar": "O/extracted/c/OUTCAR"}]},
    ]
    fetched.write_text("".join(json.dumps(e) + "\n" for e in fentries))
    return ds, raw, keep, fetched


def _load(ds: Path) -> dict:
    return {r["calc_id"]: r for r in
            (json.loads(x) for x in (ds / "metadata.jsonl").read_text().splitlines())}


def _shard_md5(ds: Path) -> dict:
    return {p.name: hashlib.md5(p.read_bytes()).hexdigest()
            for p in sorted(ds.glob("shard-*.extxyz.gz"))}


def test_keeplist_selects_only_vasprun_records(tmp_path):
    ds, raw, keep, _ = _build(tmp_path)
    summ = build_vasprun_keeplist(ds, keep, tmp_path / "vk.jsonl")
    assert summ["ok"] and summ["vasprun_records_targeted"] == 1 and summ["records_written"] == 1
    recids = {json.loads(x)["recid"] for x in (tmp_path / "vk.jsonl").read_text().splitlines()}
    assert recids == {"V"}       # NOT the OUTCAR record O


def test_refresh_adds_parameters_block_only_to_vasprun(tmp_path):
    _pmg_or_skip()
    ds, raw, keep, fetched = _build(tmp_path)
    before_keys = {cid: (tuple(r["frame_ids"]), tuple(r["shards"]))
                   for cid, r in _load(ds).items()}
    before_shards = _shard_md5(ds)
    o_cp_before = _load(ds)["zenodo:O:c/OUTCAR"]["calc_parameters"]

    summ = refresh_vasprun_metadata(ds, fetched, raw)
    assert summ["ok"] and summ["records_refreshed"] == 1 and summ["parse_failed"] == 0

    recs = _load(ds)
    v = recs["zenodo:V:vr/vasprun.xml"]["calc_parameters"]
    # the authoritative parameters block is now present (ENMAX->ENCUT, ISIF integer, ADDGRID)
    assert v["parameters"]["ENCUT"] == 400.0
    assert v["parameters"]["ISIF"] == 3 and v["parameters"]["ADDGRID"] is True
    # untouched fields preserved; stale resolved cache dropped
    assert v["run_type"] == "PBE+vdW-TS" and v["incar"] == {"ENCUT": 520.0, "GGA": "PE"}
    assert "resolved" not in v
    # OUTCAR record untouched, shards byte-identical, bijection keys unchanged
    assert recs["zenodo:O:c/OUTCAR"]["calc_parameters"] == o_cp_before
    assert {cid: (tuple(r["frame_ids"]), tuple(r["shards"]))
            for cid, r in recs.items()} == before_keys
    assert _shard_md5(ds) == before_shards


def test_refresh_idempotent_dry_run_and_backup(tmp_path):
    _pmg_or_skip()
    ds, raw, keep, fetched = _build(tmp_path)
    pristine = (ds / "metadata.jsonl").read_text()
    # dry-run writes nothing
    summ = refresh_vasprun_metadata(ds, fetched, raw, dry_run=True)
    assert summ["records_refreshed"] == 1
    assert (ds / "metadata.jsonl").read_text() == pristine
    assert not (ds / "metadata.jsonl.bak.pre_vasprun_refresh").exists()
    # real run keeps a one-time backup of the pristine state
    refresh_vasprun_metadata(ds, fetched, raw)
    bak = ds / "metadata.jsonl.bak.pre_vasprun_refresh"
    assert bak.exists() and bak.read_text() == pristine
    once = (ds / "metadata.jsonl").read_text()
    # second run is idempotent and does not overwrite the pristine backup
    refresh_vasprun_metadata(ds, fetched, raw)
    assert (ds / "metadata.jsonl").read_text() == once and bak.read_text() == pristine


def test_refresh_only_missing_skips_already_recovered(tmp_path):
    _pmg_or_skip()
    ds, raw, keep, fetched = _build(tmp_path)
    # pre-mark V as already having a parameters block; only_missing must skip it
    recs = _load(ds)
    recs["zenodo:V:vr/vasprun.xml"]["calc_parameters"]["parameters"] = {"ENCUT": 999.0}
    (ds / "metadata.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs.values()))
    summ = refresh_vasprun_metadata(ds, fetched, raw, only_missing=True)
    assert summ["vasprun_calcs_targeted"] == 0 and summ["records_refreshed"] == 0
    assert _load(ds)["zenodo:V:vr/vasprun.xml"]["calc_parameters"]["parameters"] == {"ENCUT": 999.0}


def test_verify_still_passes_after_refresh(tmp_path):
    _pmg_or_skip()
    from zenodo_harvest.dataset_ops import verify_dataset
    ds, raw, keep, fetched = _build(tmp_path)
    refresh_vasprun_metadata(ds, fetched, raw)
    out = verify_dataset(ds)
    assert out["ok"] is True
    assert out["integrity"]["n_frames_metadata"] == out["integrity"]["n_frames_on_disk"] == 2
