"""Offline tests for the targeted OUTCAR metadata recovery (``zenodo_harvest.outcar_recover``).

Builds a synthetic dataset — a real gzipped shard plus a ``metadata.jsonl`` with two
*impoverished* OUTCAR records (``run_type`` null, mimicking the current Zenodo dataset) and one
vasprun record that merely has an OUTCAR sibling — stages matching synthetic OUTCAR headers in
a ``raw`` tree, and exercises:

* :func:`build_outcar_keeplist` picks exactly the OUTCAR records (and flags any absent from the
  keep-list);
* :func:`refresh_outcar_metadata` overwrites ONLY the OUTCAR records' ``calc_parameters`` with
  the header-parsed values, leaves the vasprun record and every ``calc_id``/``frame_ids``/
  ``shards`` byte-identical, never touches a shard file, is idempotent, honours ``dry_run``,
  keeps a one-time backup, and recomputes the spin-derived availability flags.

Uses ASE only via the shard writer (same tier as ``test_store.py``). Run:
``python -m pytest tests/test_outcar_recover.py -q``.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

from ase import Atoms

from zenodo_harvest.outcar_recover import build_outcar_keeplist, refresh_outcar_metadata
from zenodo_harvest.store import ShardedExtxyzWriter

# A minimal but format-faithful OUTCAR header: PBEsol (+U here) with two POTCARs.
OUTCAR_HEADER = """\
 vasp.6.4.2 20Jul23
 INCAR:
   ENCUT = 500
   ISMEAR = 0
   SIGMA = 0.05
   LASPH = .TRUE.
   ISIF = 3
 POTCAR:    PAW_PBE Li_sv 10Sep2004
   TITEL  = PAW_PBE Li_sv 10Sep2004
 POTCAR:    PAW_PBE O 08Apr2002
   TITEL  = PAW_PBE O 08Apr2002
   k-points           NKPTS =     10
 Startparameter for this run:
   ISPIN  =      2    spin polarized calculation?
   LASPH  =      T    aspherical Exc in radial PAW
 Exchange correlation treatment:
   GGA     =    PS    GGA type
   LHFCALC =     F    Hartree Fock is set to
   AEXX    =    0.0000
--------------------------------------- Iteration    1(   1)  ---------------
 body we must not read
"""


def _write_shard(ds: Path, frame_ids: list[str]) -> None:
    """Write one real gzipped shard whose frames carry ``frame_ids`` (so verify's bijection
    and the 'shards untouched' snapshot are meaningful)."""
    with ShardedExtxyzWriter(ds) as xyz:
        for fid in frame_ids:
            a = Atoms("H", positions=[[0, 0, 0]], cell=[10, 10, 10], pbc=True)
            a.info["frame_id"] = fid
            a.info["calc_id"] = fid.split("#")[0]
            xyz.write(a)
        xyz.flush()


def _impoverished_outcar_cp() -> dict:
    """The OLD null OUTCAR calc_parameters, exactly as the current dataset stores it."""
    return {"code": "vasp", "run_type": None, "functional": None,
            "potcar_titels": ["PAW_PBE Li_sv 10Sep2004", "PAW_PBE O 08Apr2002"],
            "potcar_set_hash": "deadbeef", "note": "parsed from OUTCAR only; parameters limited"}


def _build_dataset(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Create raw/ + dataset/ + keep.jsonl + fetched.jsonl. Returns (ds, raw, keep, fetched)."""
    raw = tmp_path / "raw"
    ds = tmp_path / "ds"
    ds.mkdir(parents=True)

    # Stage OUTCAR headers for two OUTCAR records (R1, R2) at calc-id-consistent paths.
    for recid, sub in (("R1", "c1"), ("R2", "c2")):
        d = raw / recid / "extracted" / sub
        d.mkdir(parents=True)
        (d / "OUTCAR").write_text(OUTCAR_HEADER)
    # A vasprun record (R3) whose unit ALSO has an OUTCAR sibling (must NOT be refreshed).
    d3 = raw / "R3" / "extracted" / "v"
    d3.mkdir(parents=True)
    (d3 / "OUTCAR").write_text(OUTCAR_HEADER)
    (d3 / "vasprun.xml").write_text("<modeling/>")

    # metadata.jsonl: two impoverished OUTCAR records + one already-rich vasprun record.
    frame_ids = ["zenodo:R1:c1/OUTCAR#0", "zenodo:R2:c2/OUTCAR#0", "zenodo:R3:v/vasprun.xml#0"]
    _write_shard(ds, frame_ids)
    recs = [
        {"calc_id": "zenodo:R1:c1/OUTCAR", "parser": "ase.OUTCAR",
         "calc_parameters": _impoverished_outcar_cp(),
         "quality": {"n_frames": 1}, "frame_ids": ["zenodo:R1:c1/OUTCAR#0"],
         "shards": ["shard-00000.extxyz.gz"],
         "availability": {"charge_density": True, "spin_density": False, "magnetization": False},
         "site_magmoms_present": False,
         "provenance": {"source": "zenodo", "record_id": "R1", "license": "cc-by-4.0"}},
        {"calc_id": "zenodo:R2:c2/OUTCAR", "parser": "ase.OUTCAR",
         "calc_parameters": _impoverished_outcar_cp(),
         "quality": {"n_frames": 1}, "frame_ids": ["zenodo:R2:c2/OUTCAR#0"],
         "shards": ["shard-00000.extxyz.gz"],
         "availability": {"charge_density": False, "spin_density": False, "magnetization": False},
         "site_magmoms_present": False,
         "provenance": {"source": "zenodo", "record_id": "R2", "license": "cc-by-4.0"}},
        {"calc_id": "zenodo:R3:v/vasprun.xml", "parser": "pymatgen.Vasprun",
         "calc_parameters": {"code": "vasp", "run_type": "PBE", "functional": "PBE",
                             "incar": {"ENCUT": 400}},
         "quality": {"n_frames": 1}, "frame_ids": ["zenodo:R3:v/vasprun.xml#0"],
         "shards": ["shard-00000.extxyz.gz"],
         "availability": {"charge_density": False},
         "provenance": {"source": "zenodo", "record_id": "R3", "license": "cc-by-4.0"}},
    ]
    (ds / "metadata.jsonl").write_text("".join(json.dumps(r) + "\n" for r in recs))

    # keep.jsonl: R1 + R2 present (R3 too, though vasprun); recid R2 has a bytes_total.
    keep = tmp_path / "keep.jsonl"
    keep.write_text("".join(json.dumps({
        "recid": r, "bytes_total": 1000, "files": [{"key": "OUTCAR", "download": "http://x"}],
    }) + "\n" for r in ("R1", "R2", "R3")))

    # fetched.jsonl: the re-fetch result (all three staged).
    fetched = tmp_path / "fetched.jsonl"
    fentries = [
        {"recid": "R1", "provenance": {"source": "zenodo", "record_id": "R1"},
         "local_dir": "R1", "calc_units": [{"dir": "R1/extracted/c1",
                                            "outcar": "R1/extracted/c1/OUTCAR"}]},
        {"recid": "R2", "provenance": {"source": "zenodo", "record_id": "R2"},
         "local_dir": "R2", "calc_units": [{"dir": "R2/extracted/c2",
                                            "outcar": "R2/extracted/c2/OUTCAR"}]},
        {"recid": "R3", "provenance": {"source": "zenodo", "record_id": "R3"},
         "local_dir": "R3", "calc_units": [{"dir": "R3/extracted/v",
                                            "vasprun": "R3/extracted/v/vasprun.xml",
                                            "outcar": "R3/extracted/v/OUTCAR"}]},
    ]
    fetched.write_text("".join(json.dumps(e) + "\n" for e in fentries))
    return ds, raw, keep, fetched


def _load(ds: Path) -> dict:
    return {r["calc_id"]: r for r in
            (json.loads(x) for x in (ds / "metadata.jsonl").read_text().splitlines())}


def _shard_md5(ds: Path) -> dict:
    return {p.name: hashlib.md5(p.read_bytes()).hexdigest()
            for p in sorted(ds.glob("shard-*.extxyz.gz"))}


# --- build_outcar_keeplist -------------------------------------------------------------

def test_keeplist_selects_only_outcar_records(tmp_path):
    ds, raw, keep, _ = _build_dataset(tmp_path)
    out = tmp_path / "outcar_keep.jsonl"
    summ = build_outcar_keeplist(ds, keep, out)
    assert summ["ok"]
    assert summ["outcar_records_targeted"] == 2      # R1, R2 (NOT the vasprun R3)
    assert summ["records_written"] == 2
    assert summ["outcar_calcs_targeted"] == 2
    assert summ["missing_from_keep"] == []
    recids = {json.loads(x)["recid"] for x in out.read_text().splitlines()}
    assert recids == {"R1", "R2"}


def test_keeplist_flags_records_missing_from_keep(tmp_path):
    ds, raw, keep, _ = _build_dataset(tmp_path)
    # keep-list without R2
    keep2 = tmp_path / "keep2.jsonl"
    keep2.write_text(json.dumps({"recid": "R1", "bytes_total": 1, "files": []}) + "\n")
    summ = build_outcar_keeplist(ds, keep2, tmp_path / "o.jsonl")
    assert summ["records_written"] == 1 and summ["missing_from_keep"] == ["R2"]


# --- refresh_outcar_metadata -----------------------------------------------------------

def test_refresh_updates_only_outcar_records_and_preserves_shards(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    before_keys = {cid: (tuple(r["frame_ids"]), tuple(r["shards"]))
                   for cid, r in _load(ds).items()}
    before_shards = _shard_md5(ds)
    vr_cp_before = _load(ds)["zenodo:R3:v/vasprun.xml"]["calc_parameters"]

    summ = refresh_outcar_metadata(ds, fetched, raw)
    assert summ["ok"]
    assert summ["records_refreshed"] == 2            # only the two OUTCAR records
    assert summ["header_parse_failed"] == 0
    assert summ["not_yet_refetched"] == 0

    recs = _load(ds)
    # OUTCAR records now rich (PBEsol from GGA=PS), user INCAR/effective params present
    for cid in ("zenodo:R1:c1/OUTCAR", "zenodo:R2:c2/OUTCAR"):
        cp = recs[cid]["calc_parameters"]
        assert cp["run_type"] == "PBEsol" and cp["functional"] == "PBEsol"
        assert cp["encut"] == 500 and cp["spin_polarized"] is True
        assert cp["parsed_from"] == "outcar_header"
        assert "incar" in cp and "parameters" in cp

    # the vasprun record (OUTCAR sibling) is UNTOUCHED
    assert recs["zenodo:R3:v/vasprun.xml"]["calc_parameters"] == vr_cp_before

    # the bijection keys and the shard bytes are byte-identical (shards never opened)
    after_keys = {cid: (tuple(r["frame_ids"]), tuple(r["shards"])) for cid, r in recs.items()}
    assert after_keys == before_keys
    assert _shard_md5(ds) == before_shards


def test_refresh_recomputes_spin_availability(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    refresh_outcar_metadata(ds, fetched, raw)
    recs = _load(ds)
    # R1 had charge_density=True + ISPIN==2 -> spin_density True, magnetization True
    a1 = recs["zenodo:R1:c1/OUTCAR"]["availability"]
    assert a1["spin_density"] is True and a1["magnetization"] is True
    # R2 had charge_density=False -> spin_density stays False; magnetization True (spin-polarised)
    a2 = recs["zenodo:R2:c2/OUTCAR"]["availability"]
    assert a2["spin_density"] is False and a2["magnetization"] is True


def test_refresh_is_idempotent(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    refresh_outcar_metadata(ds, fetched, raw)
    first = (ds / "metadata.jsonl").read_text()
    summ2 = refresh_outcar_metadata(ds, fetched, raw)
    assert summ2["records_refreshed"] == 2
    assert (ds / "metadata.jsonl").read_text() == first     # second run is a no-op change


def test_refresh_dry_run_writes_nothing(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    before = (ds / "metadata.jsonl").read_text()
    summ = refresh_outcar_metadata(ds, fetched, raw, dry_run=True)
    assert summ["records_refreshed"] == 2                   # reports what WOULD change
    assert (ds / "metadata.jsonl").read_text() == before    # ...but nothing written
    assert not (ds / "metadata.jsonl.bak.pre_outcar_refresh").exists()


def test_refresh_keeps_one_time_backup(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    pristine = (ds / "metadata.jsonl").read_text()
    refresh_outcar_metadata(ds, fetched, raw)
    bak = ds / "metadata.jsonl.bak.pre_outcar_refresh"
    assert bak.exists() and bak.read_text() == pristine     # captured the pre-refresh state
    # a second run must NOT overwrite the pristine backup with already-refreshed content
    refresh_outcar_metadata(ds, fetched, raw)
    assert bak.read_text() == pristine


def test_refresh_only_missing_skips_already_done(tmp_path):
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    # Pre-mark R1 as already refreshed (give it a run_type); only_missing must skip it.
    recs = _load(ds)
    recs["zenodo:R1:c1/OUTCAR"]["calc_parameters"] = {"run_type": "PBE", "functional": "PBE"}
    (ds / "metadata.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in recs.values()))
    summ = refresh_outcar_metadata(ds, fetched, raw, only_missing=True)
    assert summ["records_refreshed"] == 1                   # only R2
    assert _load(ds)["zenodo:R1:c1/OUTCAR"]["calc_parameters"]["run_type"] == "PBE"  # untouched


def test_verify_still_passes_after_refresh(tmp_path):
    from zenodo_harvest.dataset_ops import verify_dataset
    ds, raw, keep, fetched = _build_dataset(tmp_path)
    refresh_outcar_metadata(ds, fetched, raw)
    out = verify_dataset(ds)
    assert out["ok"] is True
    assert out["integrity"]["n_frames_metadata"] == out["integrity"]["n_frames_on_disk"] == 3
    assert out["stats"]["frames_by_functional"].get("PBEsol") == 2   # the two refreshed OUTCARs
