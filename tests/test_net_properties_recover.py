"""Tests for the net-properties recovery (``zenodo_harvest.net_properties_recover``).

Covers the text-level shard surgery (the delicate part): after applying the map, every frame
gains ``total_magnetization``/``total_charge``, the per-atom ``dft_*`` columns are gone, all
other values (energy/forces/positions) survive a round-trip, the metadata↔shard ``frame_id``
bijection still holds (``verify``), and re-applying is a no-op (idempotent). Needs ASE (to write
real extxyz shards); pymatgen is not required for Phase 2.

Run: ``python -m pytest tests/test_net_properties_recover.py -q``.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("ase")

import numpy as np
from ase import Atoms

from zenodo_harvest import net_properties_recover as NP
from zenodo_harvest.store import ShardedExtxyzWriter, read_shard_frames_lenient


def _frame(calc_id, idx, *, energy, forces, dft=False):
    a = Atoms("H2", positions=[[0, 0, 0], [0.0, 0.0, 0.74]], cell=[10, 10, 10], pbc=True)
    a.info.update({"source": "zenodo", "calc_id": calc_id, "frame_id": f"{calc_id}#{idx}",
                   "ionic_step": idx, "REF_energy": energy})
    a.arrays["REF_forces"] = np.asarray(forces, dtype=float)
    if dft:  # a vasprun+OUTCAR final frame carried these per-atom arrays (to be stripped)
        a.arrays["dft_magmom"] = np.asarray([0.5, -0.5])
        a.arrays["dft_charge"] = np.asarray([0.11, -0.11])
    return a


def _build_dataset(tmp_path):
    """Two calcs, 3 frames total; the last frame of calc A carries per-atom dft arrays."""
    ds = tmp_path / "dataset"
    ds.mkdir()
    cid_a = "zenodo:1:calc/vasprun.xml"
    cid_b = "zenodo:2:calc/OUTCAR"
    frames = [
        _frame(cid_a, 0, energy=-1.2345678, forces=[[0.111, 0.222, 0.333], [-0.111, -0.222, -0.333]]),
        _frame(cid_a, 1, energy=-1.4000001, forces=[[0.01, 0.02, 0.03], [-0.01, -0.02, -0.03]], dft=True),
        _frame(cid_b, 0, energy=-2.5, forces=[[0.0, 0.0, 0.0], [0.0, 0.0, 0.0]]),
    ]
    with ShardedExtxyzWriter(ds) as w:
        shard_names = {f.info["frame_id"]: w.write(f) for f in frames}
    meta = [
        {"calc_id": cid_a, "parser": "pymatgen.Vasprun",
         "frame_ids": [f"{cid_a}#0", f"{cid_a}#1"], "shards": sorted(set(shard_names.values())),
         "site_magmoms_present": True, "site_charges_present": True,
         "provenance": {"record_id": "1", "source": "zenodo"},
         "availability": {"magnetization": True}},
        {"calc_id": cid_b, "parser": "ase.OUTCAR",
         "frame_ids": [f"{cid_b}#0"], "shards": sorted(set(shard_names.values())),
         "site_magmoms_present": False, "site_charges_present": False,
         "provenance": {"record_id": "2", "source": "zenodo"},
         "availability": {"magnetization": False}},
    ]
    (ds / "metadata.jsonl").write_text("".join(json.dumps(m) + "\n" for m in meta))
    npmap = tmp_path / "net_properties.jsonl"
    npmap.write_text(
        json.dumps({"calc_id": cid_a, "electronic": {"net_magnetization": 2.0, "net_charge": 0.0,
                                                     "magnetization_source": "occupancies"}}) + "\n"
        + json.dumps({"calc_id": cid_b, "electronic": {"net_magnetization": 1.5, "net_charge": -1.0,
                                                      "magnetization_source": "outcar"}}) + "\n")
    return ds, npmap, (cid_a, cid_b)


def test_apply_adds_totals_strips_dft_and_preserves_values(tmp_path):
    ds, npmap, (cid_a, cid_b) = _build_dataset(tmp_path)
    res = NP.apply_net_properties(ds, npmap)
    assert res["ok"] and res["shards_rewritten"] == 1 and res["records_refreshed"] == 2

    from zenodo_harvest.store import existing_shard_paths
    frames, trunc = read_shard_frames_lenient(existing_shard_paths(ds)[0])
    assert not trunc and len(frames) == 3
    by_id = {f.info["frame_id"]: f for f in frames}

    # every frame gained the two totals, broadcast from its calc's block
    for fid in (f"{cid_a}#0", f"{cid_a}#1"):
        assert by_id[fid].info["total_magnetization"] == 2.0
        assert by_id[fid].info["total_charge"] == 0.0
    assert by_id[f"{cid_b}#0"].info["total_magnetization"] == 1.5
    assert by_id[f"{cid_b}#0"].info["total_charge"] == -1.0

    # per-atom dft arrays are gone everywhere
    for f in frames:
        assert "dft_magmom" not in f.arrays and "dft_charge" not in f.arrays

    # untouched values survived exactly (energy + forces of the dft-bearing frame)
    f1 = by_id[f"{cid_a}#1"]
    assert f1.info["REF_energy"] == pytest.approx(-1.4000001)
    assert np.allclose(f1.arrays["REF_forces"], [[0.01, 0.02, 0.03], [-0.01, -0.02, -0.03]])


def test_apply_updates_metadata_and_verify_passes(tmp_path):
    verify = pytest.importorskip("zenodo_harvest.dataset_ops").verify_dataset
    ds, npmap, (cid_a, _cid_b) = _build_dataset(tmp_path)
    NP.apply_net_properties(ds, npmap)

    recs = {r["calc_id"]: r for r in
            (json.loads(line) for line in (ds / "metadata.jsonl").read_text().splitlines())}
    assert recs[cid_a]["electronic"]["net_magnetization"] == 2.0
    assert "site_magmoms_present" not in recs[cid_a] and "site_charges_present" not in recs[cid_a]
    # backup taken
    assert (ds / "metadata.jsonl.bak.pre_net_properties").is_file()
    # the metadata<->shard frame_id bijection still holds
    assert verify(ds)["ok"]


def test_apply_is_idempotent(tmp_path):
    ds, npmap, _ = _build_dataset(tmp_path)
    NP.apply_net_properties(ds, npmap)
    from zenodo_harvest.store import existing_shard_paths
    first = existing_shard_paths(ds)[0].read_bytes()
    # second run: shards already marked done -> skipped; metadata rewrite is a no-op
    res2 = NP.apply_net_properties(ds, npmap)
    assert res2["shards_skipped_done"] == 1 and res2["shards_rewritten"] == 0
    # and even forcing a re-read (clear the marker) reproduces byte-identical content
    (ds / NP._APPLIED_MARKER).unlink()
    NP.apply_net_properties(ds, npmap)
    assert existing_shard_paths(ds)[0].read_bytes() == first


def test_strip_dft_columns_preserves_kept_tokens():
    comment = ('Lattice="10 0 0 0 10 0 0 0 10" '
               'Properties=species:S:1:pos:R:3:REF_forces:R:3:dft_magmom:R:1:dft_charge:R:1 '
               'REF_energy=-1.4 calc_id="zenodo:1:c/vasprun.xml"')
    rows = ["H 0.0 0.0 0.0 0.01 0.02 0.03 0.5 0.11",
            "H 0.0 0.0 0.74 -0.01 -0.02 -0.03 -0.5 -0.11"]
    c2, r2, changed = NP._strip_dft_columns(comment, rows)
    assert changed
    assert "dft_magmom" not in c2 and "dft_charge" not in c2
    assert "Properties=species:S:1:pos:R:3:REF_forces:R:3 " in c2
    # kept columns (species + 3 pos + 3 forces = 7) verbatim; dft columns (last 2) dropped
    assert r2[0].split() == ["H", "0.0", "0.0", "0.0", "0.01", "0.02", "0.03"]
    # a frame without dft columns is returned unchanged
    plain = "Properties=species:S:1:pos:R:3 x"
    assert NP._strip_dft_columns(plain, ["H 0 0 0"]) == (plain, ["H 0 0 0"], False)


def test_compute_net_properties_resumes(tmp_path, monkeypatch):
    # Phase 1 without the network: a fetched manifest + a stub electronic_block_for_unit.
    raw = tmp_path / "raw"
    (raw / "1" / "extracted" / "calc").mkdir(parents=True)
    vasp = raw / "1" / "extracted" / "calc" / "vasprun.xml"
    vasp.write_text("<modeling/>")
    fetched = tmp_path / "fetched.jsonl"
    fetched.write_text(json.dumps({
        "provenance": {"record_id": "1", "source": "zenodo"},
        "local_dir": "1",
        "calc_units": [{"dir": "1/extracted/calc", "vasprun": "1/extracted/calc/vasprun.xml"}],
    }) + "\n")

    import zenodo_harvest.parse as P
    monkeypatch.setattr(P, "electronic_block_for_unit",
                        lambda unit: {"net_magnetization": 3.0, "net_charge": 0.0})
    out = tmp_path / "net_properties.jsonl"
    r1 = NP.compute_net_properties(fetched, raw, out)
    assert r1["computed_this_pass"] == 1 and r1["map_size"] == 1
    # resume: already-computed calc is skipped, nothing recomputed
    r2 = NP.compute_net_properties(fetched, raw, out)
    assert r2["computed_this_pass"] == 0 and r2["skipped_already_computed"] == 1
