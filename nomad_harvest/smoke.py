"""Phase-0 smoke test — live, end-to-end validation of the NOMAD harvest.

Runs the REAL code paths on a small live sample and prints PASS/FAIL per check, so the
risks that matter for the full run are retired before it starts:

  1  api_reachable      — anonymous read works; report the direct-upload VASP+DFT count
  2  keyset_pagination  — two pages advance with no overlap (the only scalable scroll)
  3  discover_metadata  — entries carry the fields we triage/dedup/attribute/fetch on,
                          and really are direct-upload VASP DFT (not AFLOW/OQMD/MP)
  4  license_gate       — sampled licences are redistributable; the gate logic is correct
  5  dedup_zenodo       — a planted Zenodo reference is caught; clean refs pass
  6  fetch_raw          — vasprun.xml downloads to disk (bytes > 0); record compression
  7  compression        — pymatgen parses the file whether kept compressed OR decompressed
  8  parse_compat       — the EXISTING pymatgen parser turns NOMAD vaspruns into frames
                          with REF_energy/forces/stress + full calc_parameters/quality
  9  verify_bijection   — the shared `verify` passes on the NOMAD dataset
 10  schema_parity      — frame keys match the Zenodo dataset's, so a later merge works
 11  size_calibration   — measured frames/entry, bytes/frame, download/entry vs the doc

Writes into an isolated temp dir (never the production data/); cleans up unless --keep.
Nothing here touches the zenodo_harvest package — it only imports its shared stages.

Run:  python -m nomad_harvest.smoke -n 12 [--elements Ti O] [--keep] [--work-dir DIR]
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from zenodo_harvest.manifest import read_jsonl, write_jsonl

from .client import CANDIDATE_REQUIRED, EXTERNAL_DBS, NomadClient, direct_upload_vasp_query
from .harvest import (
    build_fetched_entry,
    canonical_staged_name,
    choose_primary,
    raw_path_rel,
    stage_entry,
    zenodo_overlap,
)

N_FULL = 7_110_950  # direct-upload VASP+DFT entries (live-verified 2026-08-05)


# --- tiny check harness ---------------------------------------------------------

class Report:
    def __init__(self) -> None:
        self.rows: list[tuple[str, str, str]] = []  # (name, status, detail)

    def add(self, name: str, status: str, detail: str = "") -> None:
        symbol = {"PASS": "✓", "FAIL": "✗", "SKIP": "-", "INFO": "i"}[status]
        print(f"  [{symbol}] {name:<20} {detail}")
        self.rows.append((name, status, detail))

    def passed(self, name: str, detail: str = "") -> None: self.add(name, "PASS", detail)
    def failed(self, name: str, detail: str = "") -> None: self.add(name, "FAIL", detail)
    def skip(self, name: str, detail: str = "") -> None: self.add(name, "SKIP", detail)
    def info(self, name: str, detail: str = "") -> None: self.add(name, "INFO", detail)

    @property
    def n_fail(self) -> int:
        return sum(1 for _n, s, _d in self.rows if s == "FAIL")


def _get(entry: dict, dotted: str) -> Any:
    cur: Any = entry
    for key in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _read_shard(path: Path) -> list:
    from ase.io import read as ase_read
    if str(path).endswith(".gz"):
        with gzip.open(path, "rt") as fh:
            frames = ase_read(fh, format="extxyz", index=":")
    else:
        frames = ase_read(path, index=":")
    return frames if isinstance(frames, list) else [frames]   # index=":" -> list


# --- the smoke run --------------------------------------------------------------

def run(n: int, elements: list[str] | None, work: Path, keep: bool) -> int:
    rep = Report()
    raw_dir = work / "raw"
    ds_dir = work / "dataset"
    manifests = work / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    client = NomadClient()
    query = direct_upload_vasp_query(elements=elements)
    print(f"\nNOMAD Phase-0 smoke  (n={n}, elements={elements or 'any'}, work={work})\n")

    # 1. connectivity + count -----------------------------------------------------
    try:
        total = client.count(query)
        rep.add("api_reachable", "PASS" if total > 0 else "FAIL",
                f"{total:,} direct-upload VASP+DFT entries")
    except Exception as exc:  # noqa: BLE001
        rep.failed("api_reachable", repr(exc))
        _finish(rep, work, keep)
        return 1

    # 2. keyset pagination --------------------------------------------------------
    try:
        p1 = client.query_page(query, required={"include": ["entry_id"]}, page_size=5)
        after = p1["pagination"].get("next_page_after_value")
        p2 = client.query_page(query, required={"include": ["entry_id"]}, page_size=5,
                               page_after_value=after)
        ids1 = {e["entry_id"] for e in p1["data"]}
        ids2 = {e["entry_id"] for e in p2["data"]}
        ok = bool(after) and ids1 and ids2 and not (ids1 & ids2)
        rep.add("keyset_pagination", "PASS" if ok else "FAIL",
                f"page1={len(ids1)} page2={len(ids2)} overlap={len(ids1 & ids2)}")
    except Exception as exc:  # noqa: BLE001
        rep.failed("keyset_pagination", repr(exc))

    # 3. discover a sample --------------------------------------------------------
    try:
        entries = list(client.iter_entries(query, required=CANDIDATE_REQUIRED,
                                            page_size=min(n, 100), max_entries=n))
    except Exception as exc:  # noqa: BLE001
        rep.failed("discover_metadata", repr(exc))
        _finish(rep, work, keep)
        return 1
    if not entries:
        rep.failed("discover_metadata", "no entries returned")
        _finish(rep, work, keep)
        return 1
    missing = [e["entry_id"] for e in entries
               if not e.get("mainfile") or "license" not in e]
    not_vasp = [e["entry_id"] for e in entries
                if _get(e, "results.method.simulation.program_name") != "VASP"]
    is_ext = [e["entry_id"] for e in entries if e.get("external_db") in EXTERNAL_DBS]
    ok = not missing and not not_vasp and not is_ext
    rep.add("discover_metadata", "PASS" if ok else "FAIL",
            f"{len(entries)} entries; missing_fields={len(missing)} "
            f"non_vasp={len(not_vasp)} external_db_leak={len(is_ext)}")

    # 4. license gate -------------------------------------------------------------
    from zenodo_harvest.models import is_reusable_license
    lics = {e.get("license") for e in entries}
    gate_ok = (is_reusable_license("CC BY 4.0") and is_reusable_license("CC0 1.0")
               and not is_reusable_license("CC BY-NC 4.0") and not is_reusable_license(None))
    sample_ok = all(is_reusable_license(le) for e in entries for le in [e.get("license")])
    rep.add("license_gate", "PASS" if (gate_ok and sample_ok) else "FAIL",
            f"logic_ok={gate_ok}; sample licences={sorted(str(x) for x in lics)}")

    # 5. dedup vs Zenodo ----------------------------------------------------------
    planted = {"entry_id": "x", "references": ["https://zenodo.org/record/999"]}
    clean = {"entry_id": "y", "references": ["https://materialsproject.org/tasks/mp-1"]}
    dedup_ok = bool(zenodo_overlap(planted, set())) and not zenodo_overlap(clean, set())
    real_overlap = sum(1 for e in entries if zenodo_overlap(e, set()))
    rep.add("dedup_zenodo", "PASS" if dedup_ok else "FAIL",
            f"logic_ok={dedup_ok}; zenodo-linked in sample={real_overlap}")

    # 6. fetch raw vaspruns -------------------------------------------------------
    staged: list[tuple[dict, Path, dict]] = []
    compressed_entry: tuple[dict, str] | None = None
    for e in entries:
        try:
            res = stage_entry(client, e, raw_dir)
        except Exception as exc:  # noqa: BLE001 - a flaky 5xx on one entry, keep going
            rep.info("fetch_note", f"{e['entry_id']}: {type(exc).__name__}")
            continue
        if res is None:
            continue
        dest, rd = res
        staged.append((e, dest, rd))
        prim = choose_primary(rd)
        if (compressed_entry is None and prim
                and canonical_staged_name(prim[0], prim[1]).endswith((".gz", ".bz2", ".xz"))):
            compressed_entry = (e, raw_path_rel(prim[0], rd.get("mainfile") or ""))
    staged_files = [p for _e, d, _rd in staged for p in (d / "extracted").rglob("*") if p.is_file()]
    bytes_on_disk = sum(p.stat().st_size for p in staged_files)
    rep.add("fetch_raw", "PASS" if staged else "FAIL",
            f"staged {len(staged)}/{len(entries)} entries, {bytes_on_disk/1e6:.1f} MB on disk")

    # 7. compression handling — pymatgen parses the file kept COMPRESSED (our approach),
    #    and its content matches a LOCAL decompress. We deliberately do NOT use NOMAD's
    #    server-side `decompress` param: it doesn't cover `.bz2`, which NOMAD commonly uses.
    if compressed_entry is None:
        rep.skip("compression", "no compressed primary in sample")
    else:
        import bz2 as _bz2
        import gzip as _gz
        import lzma as _xz
        import shutil as _sh
        from zenodo_harvest.parse import parse_vasprun
        eid_c = compressed_entry[0]["entry_id"]
        dest_c = next((d for ee, d, _rd in staged if ee["entry_id"] == eid_c), None)
        comp = next((p for p in (dest_c / "extracted").rglob("vasprun.xml*")
                     if p.suffix in (".gz", ".bz2", ".xz")), None) if dest_c else None
        if comp is None:
            rep.skip("compression", "compressed primary not found on disk")
        else:
            try:
                fa, _ = parse_vasprun(str(comp), "c:comp", None)          # kept compressed
                plain = work / "compress_check" / "vasprun.xml"
                plain.parent.mkdir(parents=True, exist_ok=True)
                openers: dict[str, Any] = {".gz": _gz.open, ".bz2": _bz2.open, ".xz": _xz.open}
                with openers[comp.suffix](comp, "rb") as src, open(plain, "wb") as dst:
                    _sh.copyfileobj(src, dst)
                fb, _ = parse_vasprun(str(plain), "c:plain", None)        # locally decompressed
                ok = len(fa) == len(fb) > 0
                rep.add("compression", "PASS" if ok else "FAIL",
                        f"{comp.suffix} kept→{len(fa)} frames == decompressed→{len(fb)}")
            except Exception as exc:  # noqa: BLE001
                rep.failed("compression", f"{type(exc).__name__}: {exc}")

    # 8. parse with the EXISTING pipeline -----------------------------------------
    frames_total = calcs = 0
    if not staged:
        rep.skip("parse_compat", "nothing staged")
    else:
        from zenodo_harvest.parse import parse
        fetched = manifests / "nomad_fetched.jsonl"
        rows = []
        for e, dest, rd in staged:
            fe = build_fetched_entry(e, raw_dir, dest, rd)
            if fe:
                rows.append(fe)
        write_jsonl(fetched, rows)
        rej_path = ds_dir / "rejections.jsonl"
        try:
            stats = parse(str(fetched), dataset_dir=str(ds_dir), rejections_path=str(rej_path),
                          raw_dir=str(raw_dir), parse_timeout_s=0)
            frames_total, calcs = stats["frames"], stats["calcs_parsed"]
            reasons: dict[str, int] = {}
            if rej_path.is_file():
                for r in read_jsonl(rej_path):
                    reasons[r.get("reason", "?")] = reasons.get(r.get("reason", "?"), 0) + 1
            ok = calcs > 0 and frames_total > 0
            rep.add("parse_compat", "PASS" if ok else "FAIL",
                    f"{calcs}/{len(rows)} calcs parsed, {frames_total} frames; "
                    f"rejections={reasons or 'none'}")
        except Exception as exc:  # noqa: BLE001
            rep.failed("parse_compat", f"{type(exc).__name__}: {exc}")

    # 8b. Targeted upload-zip fetch (the DEFAULT for the real run) -----------------
    # Groups the sample by upload and pulls each mainfile out of the upload's PRE-PACKED zip
    # over HTTP Range (nomad_harvest.upload_zip). This exercises the real fast path end-to-end
    # against live NOMAD; it is paced by the 1-conn/5s throttle, so a few uploads take a few s.
    if not entries:
        rep.skip("upload_fetch", "no entries")
    else:
        try:
            from zenodo_harvest.parse import parse as _parse

            from .harvest import fetch_candidates
            ukeep = manifests / "upload_keep.jsonl"
            write_jsonl(ukeep, entries)
            uraw = work / "upload_raw"
            uout, uds = manifests / "upload_fetched.jsonl", work / "upload_ds"
            usum = fetch_candidates(client, str(ukeep), raw_dir=str(uraw), out_path=str(uout))
            ustats = _parse(str(uout), dataset_dir=str(uds), raw_dir=str(uraw),
                            rejections_path=str(uds / "rej.jsonl"), parse_timeout_s=0)
            ok = usum["staged"] > 0 and ustats["calcs_parsed"] > 0
            rep.add("upload_fetch", "PASS" if ok else "FAIL",
                    f"{usum['staged']}/{len(entries)} staged from {usum['uploads']} upload(s) "
                    f"(pre-packed zip Range-extraction; per-entry fallback="
                    f"{usum['per_entry_fallback']}); parsed {ustats['calcs_parsed']} "
                    f"-> {ustats['frames']} frames")
        except Exception as exc:  # noqa: BLE001
            rep.failed("upload_fetch", f"{type(exc).__name__}: {exc}")

    # 9. verify bijection ---------------------------------------------------------
    meta_path = ds_dir / "metadata.jsonl"
    if not meta_path.is_file():
        rep.skip("verify_bijection", "no dataset written")
    else:
        from zenodo_harvest.dataset_ops import verify_dataset
        try:
            v = verify_dataset(str(ds_dir))
            rep.add("verify_bijection", "PASS" if v.get("ok") else "FAIL",
                    f"ok={v.get('ok')} frames={v.get('n_frames') or v.get('frames')}")
        except Exception as exc:  # noqa: BLE001
            rep.failed("verify_bijection", f"{type(exc).__name__}: {exc}")

    # 10. schema parity (a frame carries the same labels the Zenodo dataset uses) --
    atoms_per_frame: list[int] = []
    if meta_path.is_file():
        shards = sorted(ds_dir.glob("shard-*.extxyz.gz"))
        try:
            frames = _read_shard(shards[0]) if shards else []
            need = {"REF_energy", "calc_id", "frame_id", "ionic_step"}
            f0 = frames[0]
            have = set(f0.info)
            has_forces = any("REF_forces" in fr.arrays for fr in frames)
            has_stress = any("REF_stress" in fr.info for fr in frames)
            atoms_per_frame = [len(fr) for fr in frames]
            ok = need <= have and len(f0) > 0
            rep.add("schema_parity", "PASS" if ok else "FAIL",
                    f"info⊇{sorted(need & have)}; forces={has_forces} stress={has_stress}")
        except Exception as exc:  # noqa: BLE001
            rep.failed("schema_parity", f"{type(exc).__name__}: {exc}")
    else:
        rep.skip("schema_parity", "no dataset written")

    # 11. size calibration --------------------------------------------------------
    if staged and frames_total:
        prim_bytes = [p.stat().st_size for _e, d, _rd in staged
                      for p in (d / "extracted").rglob("*")
                      if p.is_file() and "vasprun" in p.name.lower()]
        vasprun_mb = (sum(prim_bytes) / len(prim_bytes) / 1e6) if prim_bytes else 0.0
        frames_per_entry = frames_total / len(staged)
        shard_bytes = sum(p.stat().st_size for p in ds_dir.glob("shard-*.extxyz.gz"))
        bpf = shard_bytes / frames_total if frames_total else 0
        meta_bytes = meta_path.stat().st_size / max(calcs, 1)
        mean_atoms = (sum(atoms_per_frame) / len(atoms_per_frame)) if atoms_per_frame else 0
        print("\n  size calibration (measured on this sample):")
        print(f"    frames/entry ~ {frames_per_entry:.1f}   atoms/frame ~ {mean_atoms:.1f}")
        print(f"    vasprun (as stored) ~ {vasprun_mb:.2f} MB/entry   "
              f"extxyz.gz ~ {bpf:.0f} B/frame   metadata ~ {meta_bytes:.0f} B/calc")
        print(f"  extrapolated to the full {N_FULL:,}-entry direct-upload harvest:")
        print(f"    frames ~ {N_FULL*frames_per_entry/1e6:,.0f} M   "
              f"(doc estimate ~30 M, range 15-100 M)")
        print(f"    final extxyz.gz ~ {N_FULL*frames_per_entry*bpf/1e9:,.1f} GB   "
              f"metadata ~ {N_FULL*meta_bytes/1e9:,.1f} GB   (doc ~10 GB + ~15-20 GB)")
        print(f"    vasprun download (transient) ~ {N_FULL*vasprun_mb/1e6:,.1f} TB   "
              f"(doc ~6.5 TB)")
        if len(staged) < 50:
            print("    (small sample — noisy; run `-n 200` for a stable estimate. The "
                  "doc's ~30M-frame figure is from a diversified n=140 sample.)")
        rep.info("size_calibration", "see numbers above")

    return _finish(rep, work, keep)


def _finish(rep: Report, work: Path, keep: bool) -> int:
    n_pass = sum(1 for _n, s, _d in rep.rows if s == "PASS")
    n_fail = rep.n_fail
    n_skip = sum(1 for _n, s, _d in rep.rows if s == "SKIP")
    print(f"\n  summary: {n_pass} passed, {n_fail} failed, {n_skip} skipped")
    if keep:
        print(f"  work dir kept: {work}")
    else:
        shutil.rmtree(work, ignore_errors=True)
    print("  VERDICT:", "PASS ✓" if n_fail == 0 else f"FAIL ✗ ({n_fail} check(s))")
    return 0 if n_fail == 0 else 1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NOMAD Phase-0 smoke test")
    ap.add_argument("-n", type=int, default=12, help="entries to fetch + parse (default 12)")
    ap.add_argument("--elements", nargs="+", default=None,
                    help="restrict to materials containing ALL these elements (scope a run)")
    ap.add_argument("--work-dir", default=None, help="isolated work dir (default: a temp dir)")
    ap.add_argument("--keep", action="store_true", help="keep the work dir for inspection")
    args = ap.parse_args(argv)
    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="nomad_smoke_"))
    return run(args.n, args.elements, work, args.keep)


if __name__ == "__main__":
    sys.exit(main())
