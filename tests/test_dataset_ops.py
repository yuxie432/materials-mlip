"""Tests for the array-job dataset operations (split / merge / verify / purge-raw).

These need ``ase`` (shards are real gzipped extxyz written by ``ShardedExtxyzWriter``
and read back for integrity), so the whole module skips when ase is absent —
keeping ``test_harvest.py`` ase-free as its docstring promises.

Run: ``python -m pytest tests/test_dataset_ops.py -q`` from the repo root.
"""

from __future__ import annotations

import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("ase")

from ase import Atoms  # noqa: E402

from zenodo_harvest.dataset_ops import (  # noqa: E402
    merge_datasets,
    purge_raw,
    split_manifest,
    verify_dataset,
)
from zenodo_harvest.manifest import read_jsonl, write_jsonl  # noqa: E402
from zenodo_harvest.store import (  # noqa: E402
    MetadataWriter,
    ShardedExtxyzWriter,
    existing_shard_paths,
    next_shard_index,
)


# --------------------------------------------------------------------------- #
# Fixtures: build tiny real dataset dirs (shards + metadata records).         #
# --------------------------------------------------------------------------- #

def _build_calc(dataset_dir: Path, calc_id: str, symbols: list[str], *,
                frames_per_shard: int = 2, run_type: str = "GGA", functional: str = "PBE",
                license: str = "cc-by-4.0", resource_type: str = "dataset",
                parser: str = "pymatgen.Vasprun", electronic_converged: bool | None = True,
                n_dropped: int = 0, n_unconverged: int = 0) -> dict:
    """Append one calc (its frames + a metadata record) to ``dataset_dir``.

    Continues the dir's existing shard numbering, so repeated calls stack calcs the
    way parse does. Returns the written metadata record.
    """
    dataset_dir = Path(dataset_dir)
    start = next_shard_index(dataset_dir)
    frame_ids: list[str] = []
    shards: set[str] = set()
    with ShardedExtxyzWriter(dataset_dir, frames_per_shard, start_index=start) as xyz:
        for i, sym in enumerate(symbols):
            atoms = Atoms(sym, cell=[10, 10, 10], pbc=True)
            fid = f"{calc_id}#{i}"
            atoms.info["frame_id"] = fid
            atoms.info["calc_id"] = calc_id
            atoms.info["REF_energy"] = -1.0 * (i + 1)
            shards.add(xyz.write(atoms))
            frame_ids.append(fid)
        xyz.flush()
    rec = {
        "calc_id": calc_id,
        "frame_ids": frame_ids,
        "shards": sorted(shards),
        "parser": parser,
        "calc_parameters": {"run_type": run_type, "functional": functional},
        "provenance": {"license": license, "resource_type": resource_type},
        "quality": {"electronic_converged": electronic_converged,
                    "n_frames_scf_unconverged": n_unconverged,
                    "n_frames_dropped_no_energy": n_dropped,
                    "n_frames_with_forces": len(symbols)},
    }
    with MetadataWriter(dataset_dir / "metadata.jsonl") as mw:
        mw.write(rec)
    return rec


def _write_live_lock(dataset_dir: Path) -> None:
    """Drop a lockfile owned by THIS (alive) process into ``dataset_dir``."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    (dataset_dir / ".parse.lock").write_text(json.dumps({
        "pid": os.getpid(), "hostname": socket.gethostname(),
        "started": datetime.now(timezone.utc).isoformat(),
    }))


# --------------------------------------------------------------------------- #
# split_manifest                                                              #
# --------------------------------------------------------------------------- #

def test_split_round_robin_and_naming(tmp_path):
    manifest = tmp_path / "fetched.jsonl"
    write_jsonl(manifest, [{"recid": str(i)} for i in range(5)])
    summary = split_manifest(manifest, parts=2, out_dir=tmp_path / "parts")
    assert summary["lines_total"] == 5
    p0 = tmp_path / "parts" / "fetched.part-000.jsonl"
    p1 = tmp_path / "parts" / "fetched.part-001.jsonl"
    assert p0.is_file() and p1.is_file()
    ids0 = [r["recid"] for r in read_jsonl(p0)]
    ids1 = [r["recid"] for r in read_jsonl(p1)]
    assert ids0 == ["0", "2", "4"]           # round-robin: line i -> part i % 2
    assert ids1 == ["1", "3"]
    assert [d["lines"] for d in summary["parts_written"]] == [3, 2]


def test_split_tolerates_torn_final_line(tmp_path):
    manifest = tmp_path / "fetched.jsonl"
    manifest.write_text('{"recid": "1"}\n{"recid": "2"}\n{"recid": "3')  # crash-torn tail
    summary = split_manifest(manifest, parts=2, out_dir=tmp_path / "parts")
    assert summary["lines_total"] == 2  # torn last line dropped cleanly, no crash


# --------------------------------------------------------------------------- #
# merge_datasets                                                              #
# --------------------------------------------------------------------------- #

def test_merge_renumbers_and_rewrites_shards(tmp_path):
    dest = tmp_path / "dest"
    src = tmp_path / "src"
    _build_calc(dest, "zenodo:1:a/vasprun.xml", ["H2O", "H2", "O2"])   # -> shard 0,1
    _build_calc(src, "zenodo:2:b/vasprun.xml", ["Fe", "FeO", "Fe2O3"])  # -> shard 0,1
    assert next_shard_index(dest) == 2

    summary = merge_datasets(dest, [src])
    assert summary["ok"] is True
    assert summary["next_shard_index_start"] == 2
    assert summary["shards_moved"] == 2
    # dest now owns four contiguously-numbered shards
    assert [p.name for p in existing_shard_paths(dest)] == [
        "shard-00000.extxyz.gz", "shard-00001.extxyz.gz",
        "shard-00002.extxyz.gz", "shard-00003.extxyz.gz"]
    # the source record's `shards` list is rewritten through the rename mapping
    recs = {r["calc_id"]: r for r in read_jsonl(dest / "metadata.jsonl")}
    assert recs["zenodo:2:b/vasprun.xml"]["shards"] == [
        "shard-00002.extxyz.gz", "shard-00003.extxyz.gz"]
    assert summary["per_source"][0]["shard_map"] == {
        "shard-00000.extxyz.gz": "shard-00002.extxyz.gz",
        "shard-00001.extxyz.gz": "shard-00003.extxyz.gz"}
    assert (src / "merged.done").is_file()
    # verify agrees the merged dataset is a clean bijection
    assert verify_dataset(dest)["ok"] is True


def test_merge_refuses_duplicate_frame_id_before_any_move(tmp_path):
    dest = tmp_path / "dest"
    s1 = tmp_path / "s1"
    s2 = tmp_path / "s2"
    # same calc_id in both sources => identical frame_ids => cross-source collision
    _build_calc(s1, "zenodo:9:x/vasprun.xml", ["H", "He"])
    _build_calc(s2, "zenodo:9:x/vasprun.xml", ["H", "He"])
    summary = merge_datasets(dest, [s1, s2])
    assert summary["ok"] is False
    assert "duplicate frame_id" in summary["error"]
    # refused before moving anything: dest empty, both sources intact, no markers
    assert existing_shard_paths(dest) == []
    assert existing_shard_paths(s1) and existing_shard_paths(s2)
    assert not (s1 / "merged.done").exists() and not (s2 / "merged.done").exists()


def test_merge_refuses_missing_shard_file(tmp_path):
    dest = tmp_path / "dest"
    src = tmp_path / "src"
    _build_calc(src, "zenodo:3:c/vasprun.xml", ["H2O", "H2", "O2"])  # spans shard 0,1
    (src / "shard-00001.extxyz.gz").unlink()  # metadata still references it
    summary = merge_datasets(dest, [src])
    assert summary["ok"] is False
    assert "missing shard" in summary["error"]
    assert existing_shard_paths(dest) == []  # nothing moved


def test_merge_refuses_locked_source(tmp_path):
    dest = tmp_path / "dest"
    src = tmp_path / "src"
    _build_calc(src, "zenodo:4:d/vasprun.xml", ["H", "He"])
    _write_live_lock(src)  # a live parse lock (this test's own pid)
    summary = merge_datasets(dest, [src])
    assert summary["ok"] is False
    assert "locked" in summary["error"]
    assert existing_shard_paths(dest) == []
    assert not (src / "merged.done").exists()


def test_merge_resumes_after_midsource_crash(tmp_path):
    # Simulate a merge killed AFTER moving some of a source's shards into `into` but
    # BEFORE its marker was written (the exact stuck state the old code refused with
    # "missing shard"). A re-run must resume: skip already-moved shards, finish the
    # rest, append metadata once, and end as a clean bijection.
    import json as _json

    from zenodo_harvest.dataset_ops import MERGE_PROGRESS

    dest = tmp_path / "dest"
    src = tmp_path / "src"
    _build_calc(dest, "zenodo:1:a/vasprun.xml", ["H2O", "H2"])          # dest -> shard 0,1
    _build_calc(src, "zenodo:2:b/vasprun.xml", ["Fe", "FeO", "Fe2O3"])  # src  -> shard 0,1
    dest.mkdir(parents=True, exist_ok=True)

    # Hand-craft the interrupted state: src shard-00000 already moved to dest as
    # shard-00002 (its reserved new name), src shard-00001 NOT yet moved, marker absent,
    # progress journal present, and NO source metadata appended to dest yet.
    src_shards = [p.name for p in existing_shard_paths(src)]
    assert src_shards == ["shard-00000.extxyz.gz", "shard-00001.extxyz.gz"]
    start = next_shard_index(dest)  # 2
    mapping = {"shard-00000.extxyz.gz": f"shard-{start:05d}.extxyz.gz",
               "shard-00001.extxyz.gz": f"shard-{start+1:05d}.extxyz.gz"}
    os.replace(src / "shard-00000.extxyz.gz", dest / mapping["shard-00000.extxyz.gz"])  # partial move
    (src / MERGE_PROGRESS).write_text(_json.dumps({"into": str(dest.resolve()), "mapping": mapping}))

    summary = merge_datasets(dest, [src])
    assert summary["ok"] is True
    # resumed: only the one not-yet-moved shard was moved this run
    assert summary["per_source"][0]["shards_moved_this_run"] == 1
    assert summary["per_source"][0]["records_appended_this_run"] == 1
    assert not (src / MERGE_PROGRESS).exists()   # journal cleared on commit
    assert (src / "merged.done").is_file()
    # dest now holds a clean bijection with the source's record appended exactly once
    recs = list(read_jsonl(dest / "metadata.jsonl"))
    assert sum(1 for r in recs if r["calc_id"] == "zenodo:2:b/vasprun.xml") == 1
    assert verify_dataset(dest)["ok"] is True


def test_merge_rerun_skips_completed_source(tmp_path):
    dest = tmp_path / "dest"
    src = tmp_path / "src"
    _build_calc(src, "zenodo:5:e/vasprun.xml", ["H", "He", "Li"])
    first = merge_datasets(dest, [src])
    assert first["ok"] is True and first["shards_moved"] >= 1
    n_records_after_first = len(list(read_jsonl(dest / "metadata.jsonl")))

    second = merge_datasets(dest, [src])  # marker present -> skip, no double-append
    assert second["ok"] is True
    assert second["shards_moved"] == 0
    assert str(src) in second["sources_skipped_already_merged"]
    assert len(list(read_jsonl(dest / "metadata.jsonl"))) == n_records_after_first


# --------------------------------------------------------------------------- #
# verify_dataset                                                              #
# --------------------------------------------------------------------------- #

def test_verify_stats_and_integrity_ok(tmp_path):
    ds = tmp_path / "ds"
    _build_calc(ds, "zenodo:1:a/vasprun.xml", ["H2O", "H2"], run_type="GGA",
                functional="PBE", n_unconverged=1, n_dropped=2)
    _build_calc(ds, "zenodo:2:b/vasprun.xml", ["Fe", "FeO"], run_type="GGA+U",
                functional="PBE", electronic_converged=False, license="mit")
    out = verify_dataset(ds)
    assert out["ok"] is True
    stats = out["stats"]
    assert stats["n_calcs"] == 2
    assert stats["n_frames_metadata"] == 4 and stats["n_frames_on_disk"] == 4
    # frames weighted by each calc's frame count (2 each)
    assert stats["frames_by_functional"] == {"PBE": 4}
    assert stats["frames_by_run_type"] == {"GGA": 2, "GGA+U": 2}
    assert stats["frames_by_license"] == {"cc-by-4.0": 2, "mit": 2}
    assert stats["calcs_by_electronic_converged"] == {"true": 1, "false": 1, "null": 0}
    assert stats["total_n_frames_scf_unconverged"] == 1
    assert stats["total_n_frames_dropped_no_energy"] == 2
    assert stats["total_n_frames_with_forces"] == 4
    # element coverage: a frame counts once per element it contains
    # H: {H2O, H2}=2 ; O: {H2O}=1 ; Fe: {Fe, FeO}=2 ; O also in FeO -> O total 2
    assert stats["element_frame_counts"] == {"Fe": 2, "H": 2, "O": 2}


def test_verify_detects_metadata_referencing_absent_frame(tmp_path):
    ds = tmp_path / "ds"
    _build_calc(ds, "zenodo:1:a/vasprun.xml", ["H2O", "H2"])
    # append a metadata record whose frame_id has no frame on disk
    with MetadataWriter(ds / "metadata.jsonl") as mw:
        mw.write({"calc_id": "zenodo:99:ghost/vasprun.xml",
                  "frame_ids": ["zenodo:99:ghost/vasprun.xml#0"], "shards": []})
    out = verify_dataset(ds)
    assert out["ok"] is False
    assert "zenodo:99:ghost/vasprun.xml#0" in out["integrity"]["missing_on_disk"]
    assert out["integrity"]["n_missing_on_disk"] == 1


# --------------------------------------------------------------------------- #
# purge_raw                                                                   #
# --------------------------------------------------------------------------- #

def _raw_recid_tree(raw_dir: Path, recid: str, calc_rel: str) -> str:
    """Create ``<raw_dir>/<recid>/extracted/<calc_rel>/vasprun.xml`` and return calc_id."""
    d = raw_dir / recid / "extracted" / calc_rel
    d.mkdir(parents=True, exist_ok=True)
    (d / "vasprun.xml").write_text("<xml/>")
    (d / "OUTCAR").write_text("outcar bytes")
    return f"zenodo:{recid}:{calc_rel}/vasprun.xml"


def _fetched_record(raw_dir: Path, recid: str, calc_rels: list[str]) -> tuple[dict, list[str]]:
    units = []
    calc_ids = []
    for rel in calc_rels:
        calc_ids.append(_raw_recid_tree(raw_dir, recid, rel))
        base = Path(recid) / "extracted" / rel
        units.append({"dir": str(base), "vasprun": str(base / "vasprun.xml"),
                      "outcar": str(base / "OUTCAR")})
    rec = {"recid": recid, "provenance": {"source": "zenodo", "record_id": recid},
           "local_dir": recid, "n_calc_units": len(units), "calc_units": units}
    return rec, calc_ids


def _dataset_with_calcs(dataset_dir: Path, calc_ids: list[str]) -> None:
    for cid in calc_ids:
        _build_calc(dataset_dir, cid, ["H2O"])


def test_purge_raw_purges_fully_parsed_keeps_partial(tmp_path):
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"

    rec_full, ids_full = _fetched_record(raw, "111", ["relaxA"])
    rec_part, ids_part = _fetched_record(raw, "222", ["calc1", "calc2"])
    write_jsonl(manifests / "fetched.jsonl", [rec_full, rec_part])
    # dataset has BOTH of 111's calcs but only ONE of 222's -> 222 kept
    _dataset_with_calcs(ds, ids_full + ids_part[:1])

    summary = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")
    assert summary["ok"] is True
    decisions = {e["recid"]: e for e in summary["per_recid"]}
    assert decisions["111"]["decision"] == "purged"
    assert decisions["222"]["decision"] == "kept"
    assert decisions["222"]["n_unparsed"] == 1
    assert not (raw / "111").exists()      # fully parsed -> deleted
    assert (raw / "222").exists()          # partially parsed -> kept
    assert summary["bytes_freed"] > 0


def test_purge_raw_dry_run_deletes_nothing(tmp_path):
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"
    rec, ids = _fetched_record(raw, "111", ["relaxA"])
    write_jsonl(manifests / "fetched.jsonl", [rec])
    _dataset_with_calcs(ds, ids)

    summary = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl", dry_run=True)
    assert summary["dry_run"] is True
    assert summary["per_recid"][0]["decision"] == "purged"  # would purge...
    assert (raw / "111").exists()                           # ...but nothing deleted


def test_purge_raw_missing_manifest_errors(tmp_path):
    with pytest.raises(FileNotFoundError):
        purge_raw(tmp_path / "raw", tmp_path / "dataset",
                  fetched=tmp_path / "nope" / "fetched.jsonl")


# --------------------------------------------------------------------------- #
# parse — oversized-primary guard (an unattended batch job must not be         #
# cgroup-killed by one huge vasprun.xml, losing the whole run's progress)      #
# --------------------------------------------------------------------------- #

def _unit_and_meta(tmp_path: Path, sizes: dict[str, int]) -> tuple[dict, dict]:
    """A calc unit on disk with the given ``{role: bytes}`` primary sizes."""
    root = tmp_path / "raw" / "42" / "extracted" / "calc"
    root.mkdir(parents=True, exist_ok=True)
    names = {"vasprun": "vasprun.xml", "outcar": "OUTCAR", "vaspout": "vaspout.h5"}
    unit = {"dir": str(root)}
    for role, size in sizes.items():
        p = root / names[role]
        p.write_bytes(b"x" * size)
        unit[role] = str(p)
    base_meta = {"provenance": {"record_id": "42", "source": "zenodo"},
                 "_extracted_root": str(tmp_path / "raw" / "42" / "extracted")}
    return unit, base_meta


def test_oversized_primaries_flags_only_the_big_ones(tmp_path):
    from zenodo_harvest.parse import _oversized_primaries
    unit, _ = _unit_and_meta(tmp_path, {"vasprun": 5000, "outcar": 10})
    assert _oversized_primaries(unit, 1000) == ["vasprun"]
    assert _oversized_primaries(unit, 100_000) == []      # under the cap
    assert _oversized_primaries(unit, 0) == []            # 0 == guard disabled


def test_parse_unit_rejects_when_every_primary_is_oversized(tmp_path):
    from zenodo_harvest.manifest import RejectionLogger
    from zenodo_harvest.parse import parse_calc_unit
    unit, base_meta = _unit_and_meta(tmp_path, {"vasprun": 5000})
    rej_path = tmp_path / "rej.jsonl"
    rej = RejectionLogger(rej_path)
    assert parse_calc_unit(unit, base_meta, {}, rej, max_primary_bytes=1000) is None
    rej.close()
    rows = list(read_jsonl(rej_path))
    assert len(rows) == 1
    assert rows[0]["reason"] == "primary_too_large" and rows[0]["roles"] == ["vasprun"]
    # named under the calc_id a later uncapped re-run would parse, so it is traceable
    assert rows[0]["id"] == "zenodo:42:calc/vasprun.xml"


def test_parse_unit_falls_back_to_smaller_sibling_primary(tmp_path):
    # A huge vasprun.xml beside a modest OUTCAR: drop the vasprun, parse the OUTCAR
    # (rather than losing the calc entirely).
    from zenodo_harvest.manifest import RejectionLogger
    from zenodo_harvest.parse import parse_calc_unit
    unit, base_meta = _unit_and_meta(tmp_path, {"vasprun": 5000, "outcar": 10})
    seen = {}

    rej = RejectionLogger(tmp_path / "rej.jsonl")
    import zenodo_harvest.parse as parse_mod
    orig = parse_mod._parse_outcar_ase

    def spy(outcar_path, calc_id):
        seen["outcar"] = outcar_path
        raise RuntimeError("stop here: we only assert which path was chosen")

    parse_mod._parse_outcar_ase = spy
    try:
        parse_calc_unit(unit, base_meta, {}, rej, max_primary_bytes=1000)
    finally:
        parse_mod._parse_outcar_ase = orig
        rej.close()
    assert seen["outcar"].endswith("OUTCAR")   # fell back to the small OUTCAR


# --------------------------------------------------------------------------- #
# purge-raw partial reclaim — one unparsed unit must not pin a record's whole  #
# staging for the rest of the harvest (that accumulation can exceed the whole  #
# disk budget, leaving the pipeline's pacing loop nothing to reclaim)          #
# --------------------------------------------------------------------------- #

def test_purge_raw_frees_parsed_units_of_a_partially_parsed_record(tmp_path):
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"

    rec, ids = _fetched_record(raw, "222", ["calc1", "calc2", "calc3"])
    write_jsonl(manifests / "fetched.jsonl", [rec])
    _dataset_with_calcs(ds, ids[:2])           # calc1+calc2 parsed, calc3 not

    before = sum(1 for p in (raw / "222").rglob("*") if p.is_file())
    summary = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")
    entry = summary["per_recid"][0]

    assert entry["decision"] == "kept" and entry["n_unparsed"] == 1
    assert (raw / "222").exists()                          # record retained for the retry
    # ...but the two parsed calcs' files are gone, so their space is reclaimed
    assert not (raw / "222" / "extracted" / "calc1" / "OUTCAR").exists()
    assert not (raw / "222" / "extracted" / "calc2" / "vasprun.xml").exists()
    # ...while everything the UNPARSED calc needs survives
    assert (raw / "222" / "extracted" / "calc3" / "OUTCAR").is_file()
    assert (raw / "222" / "extracted" / "calc3" / "vasprun.xml").is_file()
    after = sum(1 for p in (raw / "222").rglob("*") if p.is_file())
    assert after < before
    assert entry["partial_files_removed"] == before - after
    assert entry["partial_bytes_freed"] > 0 and summary["bytes_freed"] > 0
    assert summary["files_removed"] == before - after


def test_purge_raw_partial_keeps_files_shared_with_an_unparsed_unit(tmp_path):
    # A flat multi-calc directory shares untagged inputs between units. A shared file must
    # never be freed while an unparsed unit still references it, or its re-parse breaks.
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"

    flat = raw / "333" / "extracted" / "flat"
    flat.mkdir(parents=True)
    for name in ("site1_OUTCAR", "site2_OUTCAR", "INCAR"):
        (flat / name).write_text("data " * 50)
    base = Path("333") / "extracted" / "flat"
    units = [{"dir": str(base), "outcar": str(base / "site1_OUTCAR"), "incar": str(base / "INCAR")},
             {"dir": str(base), "outcar": str(base / "site2_OUTCAR"), "incar": str(base / "INCAR")}]
    rec = {"recid": "333", "provenance": {"source": "zenodo", "record_id": "333"},
           "local_dir": "333", "n_calc_units": 2, "calc_units": units}
    write_jsonl(manifests / "fetched.jsonl", [rec])
    _dataset_with_calcs(ds, ["zenodo:333:flat/site1_OUTCAR"])   # only unit 1 parsed

    purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")
    assert not (flat / "site1_OUTCAR").exists()   # parsed unit's own output freed
    assert (flat / "site2_OUTCAR").is_file()      # unparsed unit intact
    assert (flat / "INCAR").is_file()             # SHARED input preserved


def test_purge_raw_partial_prunes_empty_dirs_and_dry_run_is_safe(tmp_path):
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"
    rec, ids = _fetched_record(raw, "444", ["calc1", "calc2"])
    write_jsonl(manifests / "fetched.jsonl", [rec])
    _dataset_with_calcs(ds, ids[:1])

    dry = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl", dry_run=True)
    assert dry["per_recid"][0]["partial_files_removed"] == 2      # would free calc1's files
    assert (raw / "444" / "extracted" / "calc1" / "OUTCAR").is_file()   # nothing deleted

    real = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")
    assert not (raw / "444" / "extracted" / "calc1").exists()     # emptied dir pruned
    assert real["per_recid"][0]["empty_dirs_pruned"] >= 1
    assert (raw / "444" / "extracted" / "calc2" / "OUTCAR").is_file()


def test_purge_raw_partial_is_idempotent(tmp_path):
    raw = tmp_path / "raw"
    manifests = tmp_path / "manifests"
    manifests.mkdir(parents=True)
    ds = tmp_path / "dataset"
    rec, ids = _fetched_record(raw, "555", ["calc1", "calc2"])
    write_jsonl(manifests / "fetched.jsonl", [rec])
    _dataset_with_calcs(ds, ids[:1])

    first = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")
    second = purge_raw(raw, ds, fetched=manifests / "fetched.jsonl")   # must not raise
    assert first["bytes_freed"] > 0
    assert second["bytes_freed"] == 0 and second["files_removed"] == 0
    assert (raw / "555" / "extracted" / "calc2" / "OUTCAR").is_file()
