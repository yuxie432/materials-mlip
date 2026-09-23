"""Command-line entrypoints for the Materials Cloud harvest.

Stages 0-1 (discover = full census + gates; triage = zip/AiiDA peeks + evidence policy) are
Materials-Cloud-specific; stage 2 is the SHARED ``zenodo_harvest.fetch`` driven with an anonymous
MC session; stages 3-5 (parse / store / merge / verify) are the shared, unmodified code, so the
MC dataset comes out schema-identical to the Zenodo and NOMAD ones::

    python -m materials_cloud_harvest.cli smoke                  # live end-to-end check (temp dir)
    python -m materials_cloud_harvest.cli discover               # census -> mc_candidates.jsonl
    python -m materials_cloud_harvest.cli triage                 # peeks  -> mc_keep.jsonl + report
    # one overlapped, disk-paced command (fetch unit-batch i+1 while parse+purge batch i):
    python -m materials_cloud_harvest.cli pipeline --parts 8 \
        --max-disk-bytes 300000000000 --max-disk-files 300000
    python -m materials_cloud_harvest.cli status
    python -m zenodo_harvest.cli verify --dataset-dir <MC root>/dataset

Everything lives under the MC tree (``$MC_HARVEST_DATA``; default a ``materials_cloud`` sibling
of an absolute Zenodo root, e.g. ``/rds/.../hpc-work/materials_cloud``), never mixed with the
Zenodo/NOMAD trees (each harvest's disk valve walks only its own raw dir).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from zenodo_harvest import config
from zenodo_harvest.dataset_ops import purge_raw, split_manifest, verify_dataset
from zenodo_harvest.fetch import (
    DEFAULT_MAX_MEMBER_BYTES,
    DEFAULT_ZIP_STREAM_MAX_FILES,
    fetch as shared_fetch,
)
from zenodo_harvest.parse import parse
from zenodo_harvest.store import DatasetLockError

from .client import MaterialsCloudClient, new_session
from .discover import discover
from .fetching import TRANSIENT_RETRIES, fetch_with_retries
from .records import LICENCE_POLICIES
from .remote_zip import DEFAULT_MAX_CD_BYTES
from .triage import DEFAULT_FILE_ALLOWLIST, triage

CANDIDATES = "mc_candidates.jsonl"
KEEP = "mc_keep.jsonl"
REJECTIONS = "mc_rejections.jsonl"          # discover + triage drops (auditable recall)
FETCH_REJECTIONS = "mc_fetch_rejections.jsonl"
FETCHED = "mc_fetched.jsonl"


def mc_paths() -> tuple[Path, Path, Path, Path]:
    """Materials Cloud's OWN ``(root, manifests, raw, dataset)`` — separate from Zenodo/NOMAD.

    Root precedence: ``$MC_HARVEST_DATA``; else a ``materials_cloud`` SIBLING of an absolute Zenodo
    root (CSD3: ``/rds/.../hpc-work/zenodo`` → ``/rds/.../hpc-work/materials_cloud``); else nested
    under the relative local default (``data/materials_cloud``, gitignored). Call after
    :func:`config.refresh_paths` so a ``.env`` root is honoured."""
    env = os.environ.get("MC_HARVEST_DATA")
    root = Path(env) if env else (
        config.DATA_ROOT.parent / "materials_cloud" if config.DATA_ROOT.is_absolute()
        else config.DATA_ROOT / "materials_cloud")
    return root, root / "manifests", root / "raw", root / "dataset"


def _default_nomad_metadata() -> Path | None:
    """The NOMAD dataset's metadata.jsonl (overlap flags), if that tree exists here."""
    try:
        from nomad_harvest.cli import nomad_paths
    except Exception:  # noqa: BLE001 - optional
        return None
    p = nomad_paths()[3] / "metadata.jsonl"
    return p if p.is_file() else None


def _parse_allow(values: list[str] | None, use_default: bool) -> dict[str, tuple[str, ...]]:
    allow: dict[str, list[str]] = ({k: list(v) for k, v in DEFAULT_FILE_ALLOWLIST.items()}
                                   if use_default else {})
    for v in values or []:
        rid, sep, glob = v.partition("=")
        if not sep or not rid or not glob:
            raise SystemExit(f"--allow expects RECID=GLOB, got {v!r}")
        allow.setdefault(rid, []).append(glob)
    return {k: tuple(v) for k, v in allow.items()}


def _add_fetch_opts(p: argparse.ArgumentParser) -> None:
    p.add_argument("--max-bytes", type=int, default=0,
                   help="per-file download cap; 0 = uncapped (the harvest default — the disk valve "
                        "is the bound)")
    p.add_argument("--max-member-bytes", type=int, default=DEFAULT_MAX_MEMBER_BYTES,
                   help="cap on each EXTRACTED file (decompression-bomb guard); 0 = no cap")
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="disk budget for the whole MC raw staging dir (0 = no limit)")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="inode budget for the MC raw staging dir (0 = no limit); CSD3 /rds counts "
                        "files AND directories against its 1M-inode quota")
    p.add_argument("--workers", type=int, default=4,
                   help="concurrent fetch units (MC allows 500 req/min; downloads come from CSCS S3)")
    p.add_argument("--no-zip-stream", dest="zip_stream", action="store_false",
                   help="disable targeted ZIP member fetch (on by default; ZIP64 archives always "
                        "fall back to a whole download + selective extraction)")
    p.add_argument("--zip-stream-max-files", type=int, default=DEFAULT_ZIP_STREAM_MAX_FILES)
    p.add_argument("--retry-rejected", action="store_true",
                   help="re-attempt fetch units previously rejected as terminal")
    p.add_argument("--transient-retries", type=int, default=TRANSIENT_RETRIES,
                   help="extra fetch passes over units that failed TRANSIENTLY (network drop, "
                        "5xx, 429) — each resumes the kept .part over HTTP Range (default "
                        f"{TRANSIENT_RETRIES}; 0 = leave them to the next run)")


def _fetch(args: argparse.Namespace, in_path: str, out_path: str, raw_dir: str,
           rejections: str, max_records: int | None = None,
           retry_rejected: bool | None = None) -> dict:
    """The shared Zenodo fetch, driven with an anonymous Materials Cloud session, re-run while
    units fail transiently (see :mod:`fetching` — a one-job MC harvest must not leave a unit
    whose download dropped once silently unfetched). No retry passes under ``--max-records``."""
    first_retry = args.retry_rejected if retry_rejected is None else retry_rejected

    def _once(retry_rejected: bool = first_retry) -> dict:
        return shared_fetch(
            in_path, out_path=out_path, raw_dir=raw_dir, rejections_path=rejections,
            max_bytes=None if args.max_bytes == 0 else args.max_bytes,
            max_records=max_records, retry_rejected=retry_rejected,
            max_member_bytes=args.max_member_bytes if args.max_member_bytes > 0 else (1 << 62),
            max_disk_bytes=args.max_disk_bytes or None,
            max_disk_files=args.max_disk_files or None,
            workers=args.workers, zip_stream=args.zip_stream,
            zip_stream_max_files=args.zip_stream_max_files, session_factory=new_session)
    if max_records:
        return _once()
    # only the FIRST pass honours --retry-rejected; retry passes never re-attempt TERMINAL
    # rejections, so a proven-empty unit is not re-downloaded on every pass
    n_calls = [0]

    def _pass() -> dict:
        n_calls[0] += 1
        return _once(first_retry if n_calls[0] == 1 else False)
    return fetch_with_retries(_pass, in_path, out_path, rejections,
                              retries=args.transient_retries)


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    config.refresh_paths()
    _root, man, raw, ds = mc_paths()
    p = argparse.ArgumentParser(prog="materials_cloud_harvest", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("discover", help="stage 0: full census of every MC record -> gated, "
                                        "overlap-flagged candidate manifest")
    d.add_argument("--out", default=str(man / CANDIDATES))
    d.add_argument("--rejections", default=str(man / REJECTIONS))
    d.add_argument("--licence-policy", choices=LICENCE_POLICIES, default="nc-ok",
                   help="nc-ok (default): keep CC0/BY/BY-SA/permissive + NC/NC-SA, drop ND, "
                        "no-licence, mcloud-ne-1.0, asl; strict: also drop NC; none: keep all")
    d.add_argument("--exclude-id", action="append", default=[], help="record id to drop (repeat)")
    d.add_argument("--only-id", action="append", default=None,
                   help="discover just these record ids (repeat) instead of the full census")
    d.add_argument("--zenodo-metadata", default=str(config.DATASET_DIR / "metadata.jsonl"),
                   help="Zenodo dataset metadata.jsonl for overlap FLAGS (skipped if absent)")
    d.add_argument("--nomad-metadata", default=None,
                   help="NOMAD dataset metadata.jsonl for overlap FLAGS (default: the NOMAD tree's, "
                        "if present); 'none' to skip")
    d.add_argument("--drop-linked", action="store_true",
                   help="DROP records linked to a harvested Zenodo record instead of flagging "
                        "(off: MC->Zenodo links are IsSupplementTo, i.e. complementary data)")
    d.add_argument("--max-records", type=int, default=None)

    t = sub.add_parser("triage", help="stage 1: peek zips/AiiDA exports, apply the evidence "
                                      "policy -> fetch-unit keep-list + census report")
    t.add_argument("--in", dest="in_path", default=str(man / CANDIDATES))
    t.add_argument("--out", default=str(man / KEEP))
    t.add_argument("--report", default=None, help="default: <out stem>.report.json")
    t.add_argument("--rejections", default=str(man / REJECTIONS))
    t.add_argument("--min-rank", type=int, default=3)
    t.add_argument("--no-peek", dest="peek", action="store_false")
    t.add_argument("--max-cd-bytes", type=int, default=DEFAULT_MAX_CD_BYTES,
                   help="largest central directory to read per archive (AiiDA exports can have "
                        "10^5+ members); larger ones are reported as evidence gaps")
    t.add_argument("--allow", action="append", default=None,
                   help="restrict a record to matching files: RECID=GLOB (repeatable)")
    t.add_argument("--no-default-allowlist", action="store_true",
                   help="drop the built-in allowlist (Bosoni ACWF: *_results_vasp.aiida only)")
    t.add_argument("--no-split", dest="split", action="store_false",
                   help="one keep-list entry per record instead of per archive")
    t.add_argument("--interval", type=float, default=0.4,
                   help="min seconds between peek STARTS across all peek workers (keeps several "
                        "parallel peeks well under MC's 500 requests/60 s)")
    t.add_argument("--peek-workers", type=int, default=4,
                   help="concurrent central-directory peeks (they are request-latency-bound, "
                        "~1-3 small Range reads each through MC's API redirect)")
    t.add_argument("--max-records", type=int, default=None)

    f = sub.add_parser("fetch", help="stage 2: shared fetch of the keep-list (anonymous MC session)")
    f.add_argument("--in", dest="in_path", default=str(man / KEEP))
    f.add_argument("--out", default=str(man / FETCHED))
    f.add_argument("--raw-dir", default=str(raw))
    f.add_argument("--rejections", default=str(man / FETCH_REJECTIONS))
    f.add_argument("--max-records", type=int, default=None)
    _add_fetch_opts(f)

    pi = sub.add_parser("pipeline", help="stages 2-4 overlapped (fetch i+1 || parse+purge i) + "
                                         "verify, paced by the disk/inode valve")
    pi.add_argument("--in", dest="in_path", default=str(man / KEEP))
    pi.add_argument("--parts", type=int, required=True)
    pi.add_argument("--parts-dir", default=None,
                    help="dir for the split parts (default: <in>.pipeline_parts/)")
    pi.add_argument("--raw-dir", default=str(raw))
    pi.add_argument("--dataset-dir", default=str(ds))
    pi.add_argument("--max-primary-bytes", type=int, default=0,
                    help="RAM guard: skip (primary_too_large) any vasprun/OUTCAR above this "
                         "UNCOMPRESSED size (0 = off); pymatgen peak RSS ~10-12x the file")
    pi.add_argument("--parse-timeout", type=float, default=1200)
    pi.add_argument("--parse-workers", type=int, default=1)
    _add_fetch_opts(pi)

    st = sub.add_parser("status", help="read-only progress snapshot of the MC harvest")
    st.add_argument("--manifests-dir", default=str(man))
    st.add_argument("--raw-dir", default=str(raw))
    st.add_argument("--dataset-dir", default=str(ds))
    st.add_argument("--keep", default=None)
    st.add_argument("--max-disk-bytes", type=int, default=0)
    st.add_argument("--max-disk-files", type=int, default=0)
    st.add_argument("--no-staging-walk", action="store_true")
    st.add_argument("--json", action="store_true")

    s = sub.add_parser("smoke", help="live end-to-end validation on tiny records (temp dir)")
    s.add_argument("--record", action="append", default=None,
                   help="record id(s) to run (default: ydn09-ngs56, a 16.5 MB HSE06 OUTCAR zip)")
    s.add_argument("--work-dir", default=None)
    s.add_argument("--keep", action="store_true", help="keep the work dir afterwards")

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "smoke":
        import tempfile
        from .smoke import run
        work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="mc_smoke_"))
        return run(args.record or ["ydn09-ngs56"], work, args.keep)
    if args.cmd == "status":
        return _status(args)
    if args.cmd == "discover":
        zmeta = args.zenodo_metadata if Path(args.zenodo_metadata).is_file() else None
        if args.nomad_metadata == "none":
            nmeta = None
        elif args.nomad_metadata:
            nmeta = args.nomad_metadata if Path(args.nomad_metadata).is_file() else None
        else:
            nmeta = _default_nomad_metadata()
        summary = discover(MaterialsCloudClient(), args.out, rejections_path=args.rejections,
                           licence_policy=args.licence_policy, exclude_ids=args.exclude_id,
                           only_ids=args.only_id, zenodo_metadata=zmeta, nomad_metadata=nmeta,
                           drop_linked=args.drop_linked, max_records=args.max_records)
    elif args.cmd == "triage":
        summary = triage(args.in_path, args.out, report_path=args.report,
                         rejections_path=args.rejections, min_rank=args.min_rank,
                         peek=args.peek, max_cd_bytes=args.max_cd_bytes,
                         file_allowlist=_parse_allow(args.allow, not args.no_default_allowlist),
                         split=args.split, interval=args.interval, max_records=args.max_records,
                         peek_workers=args.peek_workers)
    elif args.cmd == "fetch":
        summary = _fetch(args, args.in_path, args.out, args.raw_dir, args.rejections,
                         args.max_records)
    elif args.cmd == "pipeline":
        return _run_pipeline(args)
    else:  # pragma: no cover
        p.error(f"unknown command {args.cmd}")
    print(json.dumps(summary, indent=2))
    return 0


def _status(args: argparse.Namespace) -> int:
    from zenodo_harvest.status import format_status, status_report
    report = status_report(
        manifests_dir=args.manifests_dir, raw_dir=args.raw_dir, dataset_dir=args.dataset_dir,
        keep_path=args.keep, max_disk_bytes=(args.max_disk_bytes or None),
        max_disk_files=(args.max_disk_files or None), staging_walk=not args.no_staging_walk,
        candidate_globs=[CANDIDATES], keep_name=KEEP,
        extra_rejection_names=(REJECTIONS, FETCH_REJECTIONS),
        fetched_globs=["*.fetched.jsonl", FETCHED])
    print(json.dumps(report, indent=2) if args.json else format_status(report))
    return 0


def _run_pipeline(args: argparse.Namespace) -> int:
    """Overlapped fetch || parse+purge over the fetch-unit keep-list, then verify.

    Reuses the shared, I/O-agnostic ``zenodo_harvest.pipeline.run_pipeline``: a part whose fetch
    stopped on the disk valve returns False, so the orchestrator reclaims staging (parse+purge)
    and resumes THAT part — nothing partly fetched is silently dropped."""
    from zenodo_harvest.pipeline import run_pipeline

    raw_dir, ds_dir = Path(args.raw_dir), Path(args.dataset_dir)
    if not Path(args.in_path).is_file():
        print(f"ERROR: keep-list {args.in_path} missing — run discover + triage first",
              file=sys.stderr)
        return 2
    parts_dir = Path(args.parts_dir or str(Path(args.in_path).with_suffix("")) + ".pipeline_parts")
    split_info = split_manifest(args.in_path, args.parts, parts_dir)
    part_paths = [Path(pw["path"]) for pw in split_info["parts_written"] if pw["lines"] > 0]
    fetch_rej = str(raw_dir.parent / "manifests" / FETCH_REJECTIONS)
    parse_rej = str(ds_dir / "rejections.jsonl")

    def _fetched_path(part: Path) -> Path:
        return part.with_name(part.stem + ".fetched.jsonl")

    pending: dict[str, list[str]] = {}   # part -> units still pending after the retry passes
    too_big: dict[str, list[str]] = {}   # part -> units that need a bigger staging budget
    retried: set[str] = set()            # parts whose terminal rejections were re-attempted once

    def fetch_fn(part: Path) -> bool:
        # --retry-rejected re-attempts a part's terminal units ONCE, not on every resume of the
        # part after a disk-budget pause (that would re-download proven-empty units repeatedly)
        first = args.retry_rejected and part.name not in retried
        retried.add(part.name)
        summary = _fetch(args, str(part), str(_fetched_path(part)), str(raw_dir), fetch_rej,
                         retry_rejected=first)
        pending[part.name] = list(summary.get("pending_after_retries") or [])
        too_big[part.name] = list(summary.get("units_exceeding_disk_budget") or [])
        return not summary.get("stopped_disk_budget", False)

    def process_fn(part: Path) -> None:
        fetched = str(_fetched_path(part))
        if not Path(fetched).is_file():
            return                        # nothing fetched for this part (all rejected)
        parse(fetched, dataset_dir=str(ds_dir), rejections_path=parse_rej, raw_dir=str(raw_dir),
              max_primary_bytes=args.max_primary_bytes, parse_timeout_s=args.parse_timeout,
              parse_workers=args.parse_workers)
        purge_raw(str(raw_dir), str(ds_dir), fetched=fetched)

    fetch_error: str | None = None
    done_parts: list[Path] = []
    errors: list[tuple[Path, Exception]] = []
    try:
        done_parts, errors = run_pipeline(part_paths, fetch_fn, process_fn, after_workers=1)
    except DatasetLockError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - a hard foreground fetch failure; still report+verify
        logging.getLogger(__name__).exception("materials cloud pipeline fetch failed")
        fetch_error = f"{type(exc).__name__}: {exc}"
    verify = verify_dataset(str(ds_dir)) if (ds_dir / "metadata.jsonl").is_file() else {
        "ok": False, "error": "no dataset written"}
    still_pending = sorted(u for units in pending.values() for u in units)
    # A unit still failing TRANSIENTLY after the in-run retry passes makes the run non-OK, so a
    # RESUBMIT=1 chain gives it another job (resume skips everything already done) instead of the
    # harvest "succeeding" with data silently missing.
    ok = not fetch_error and not errors and not still_pending and verify.get("ok", False)
    print(json.dumps({"parts": len(part_paths), "parts_done": len(done_parts),
                      "fetch_error": fetch_error,
                      "process_errors": [f"{pp}: {type(e).__name__}: {e}" for pp, e in errors],
                      "pending_units": still_pending,
                      # not a failure of THIS run (a resubmit could not help): raise the budget
                      "units_exceeding_disk_budget": sorted(u for us in too_big.values() for u in us),
                      "verify": verify, "ok": ok}, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
