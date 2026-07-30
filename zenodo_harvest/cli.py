"""Command-line entrypoints for the harvest pipeline.

Examples
--------
Small WSL trial (first 10k per query, no download)::

    python -m zenodo_harvest.cli discover --max-records 200 \
        --out data/manifests/candidates.jsonl
    python -m zenodo_harvest.cli triage \
        --in data/manifests/candidates.jsonl \
        --out data/manifests/keep.jsonl --min-rank 3

Triage peeks into remote .zip central directories by default (confirms VASP without
downloading); pass --no-peek to disable::

    python -m zenodo_harvest.cli triage --in data/manifests/candidates.jsonl \
        --out data/manifests/keep.jsonl

Full harvest on the cluster (recursive date partitioning past the 10k cap)::

    python -m zenodo_harvest.cli discover --exhaustive \
        --out data/manifests/candidates_full.jsonl

Parallel parse via an array job (split -> N tasks -> merge -> verify -> purge)::

    python -m zenodo_harvest.cli split --in data/manifests/fetched.jsonl \
        --parts 16 --out-dir data/manifests/parts
    # each array task: parse --in .../fetched.part-0i.jsonl --dataset-dir data/dataset/task-i
    python -m zenodo_harvest.cli merge-datasets --into data/dataset data/dataset/task-*
    python -m zenodo_harvest.cli verify --dataset-dir data/dataset
    python -m zenodo_harvest.cli purge-raw --raw-dir data/raw --dataset-dir data/dataset
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from pathlib import Path

from . import config
from .client import ZenodoClient
from .dataset_ops import merge_datasets, purge_raw, split_manifest, verify_dataset
from .discover import DEFAULT_QUERIES, DEFAULT_RESOURCE_TYPES, discover
from .fetch import DEFAULT_MAX_MEMBER_BYTES, DEFAULT_ZIP_STREAM_MAX_FILES, fetch
from .parse import parse
from .store import DatasetLockError
from .triage import triage


def _add_discover(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("discover", help="stage 0: build candidate manifest from Zenodo search")
    # Honour ZENODO_HARVEST_DATA like fetch/parse do (main() ran refresh_paths() already),
    # so an ad-hoc `discover` on the cluster writes to /rds scratch, not /home's 50 GB quota.
    p.add_argument("--out", default=str(config.MANIFEST_DIR / "candidates.jsonl"))
    p.add_argument("--query", action="append", dest="queries",
                   help="override default queries (repeatable)")
    p.add_argument("--resource-type", action="append", dest="resource_types",
                   default=None, help="resource type(s) to keep (default: dataset)")
    p.add_argument("--exhaustive", action="store_true",
                   help="recursive date-partitioning to exceed the 10k window (cluster)")
    p.add_argument("--fresh", action="store_true",
                   help="ignore + remove any <out>.hits.jsonl checkpoint (clean rebuild)")
    p.add_argument("--no-license-gate", dest="license_gate", action="store_false",
                   help="keep records with any license (default: drop NC/ND/no-license, "
                        "keeping only redistributable data)")
    p.add_argument("--max-records", type=int, default=None)
    p.add_argument("--base", default="https://zenodo.org")


def _add_triage(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("triage", help="stage 1: filter candidates to a keep-list")
    p.add_argument("--in", dest="in_path", required=True)
    p.add_argument("--out", dest="out_path", required=True)
    p.add_argument("--min-rank", type=int, default=3,
                   help="4=vasp_direct 3=+archive 2=+processed")
    p.add_argument("--no-peek", dest="peek", action="store_false",
                   help="disable remote zip central-directory inspection "
                        "(peek is ON by default — it is ~1000x cheaper than downloading)")
    p.add_argument("--keep-unconfirmed", dest="require_confirmed", action="store_false",
                   help="keep archive records even when a successful zip peek proved they "
                        "contain no VASP (default: drop those to avoid downloading them)")


def _add_fetch(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("fetch", help="stage 2: download + stage VASP files for a keep-list")
    p.add_argument("--in", dest="in_path", required=True, help="triaged keep-list JSONL")
    p.add_argument("--out", dest="out_path", default=str(config.MANIFEST_DIR / "fetched.jsonl"))
    p.add_argument("--raw-dir", default=str(config.RAW_DIR))
    p.add_argument("--rejections", default=None,
                   help="rejection log path (default: <raw-dir>/../manifests/rejections.jsonl)")
    p.add_argument("--max-bytes", type=int, default=500_000_000,
                   help="skip any single file/archive larger than this many bytes; 0 = no cap "
                        "(default 500MB). Archives are deleted after extraction, so with 0 "
                        "pipeline fetch->parse->purge-raw in batches to bound transient disk.")
    p.add_argument("--max-member-bytes", type=int, default=DEFAULT_MAX_MEMBER_BYTES,
                   help=f"skip any single EXTRACTED file larger than this; 0 = no cap "
                        f"(default {DEFAULT_MAX_MEMBER_BYTES // 10**9}GB)")
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="disk-budget valve: stop cleanly (resumable) once the raw staging "
                        "dir reaches this many bytes; 0 = no limit. Use with an uncapped "
                        "harvest paced as fetch->parse->purge-raw->fetch to bound peak disk. "
                        "Enforced on ACTUAL bytes as they are written (no decompression-ratio "
                        "guess), so it is a hard bound on the whole raw dir; leave headroom "
                        "only for the dataset dir if it shares the quota — e.g. ~0.8*quota "
                        "(800000000000 for a 1 TB quota).")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="inode-budget valve: stop cleanly (resumable) once the raw staging "
                        "dir holds this many files; 0 = no limit. On CSD3 hpc-work (1 TB AND "
                        "1M files) this binds BEFORE the byte budget — measured extracted "
                        "VASP data runs ~270KiB mean/7.6KiB median per file, so 1M files "
                        "can arrive near 0.3 TB. Enforced exactly, as files are written. "
                        "Suggest ~800000 for a 1M-file quota.")
    p.add_argument("--retry-rejected", action="store_true",
                   help="reprocess records previously rejected as terminal (e.g. after raising --max-bytes)")
    p.add_argument("--workers", type=int, default=4,
                   help="concurrent record downloads (default 4). Records are independent; "
                        "keep small to respect Zenodo's 100 req/min, 5000 req/hour limits. "
                        "The disk valve still bounds peak disk across all in-flight downloads.")
    p.add_argument("--no-zip-stream", dest="zip_stream", action="store_false",
                   help="disable targeted ZIP member fetch (pull only the VASP files out "
                        "of a .zip over HTTP Range instead of downloading the whole "
                        "archive). ON by default; a zip that is not addressable this way "
                        "(ZIP64/encrypted/odd-compression VASP member, or a small ~all-VASP "
                        "zip) falls back to a whole-archive download automatically.")
    p.add_argument("--zip-stream-max-files", type=int, default=DEFAULT_ZIP_STREAM_MAX_FILES,
                   help=f"max VASP members to pull individually from one .zip before falling "
                        f"back to a whole-archive download — each costs ~1-2 HTTP requests, "
                        f"so a many-small-member VASP dump is cheaper whole "
                        f"(default {DEFAULT_ZIP_STREAM_MAX_FILES}).")
    p.add_argument("--max-records", type=int, default=None)


def _add_parse(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("parse", help="stage 3: parse fetched calcs -> extxyz.gz + metadata.jsonl")
    p.add_argument("--in", dest="in_path", required=True, help="fetched manifest JSONL")
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))
    p.add_argument("--raw-dir", default=str(config.RAW_DIR),
                   help="where fetch staged the files; manifest paths resolve against it")
    p.add_argument("--rejections", default=None,
                   help="rejection log path (default: <dataset-dir>/../manifests/rejections.jsonl)")
    p.add_argument("--frames-per-shard", type=int, default=10_000)
    p.add_argument("--gzip-level", type=int, default=6,
                   help="gzip compression level 1-9 for shards (1=fast/large, 9=small/slow, default 6)")
    p.add_argument("--max-primary-bytes", type=int, default=0,
                   help="skip (log 'primary_too_large') any vasprun.xml/vaspout.h5/OUTCAR "
                        "bigger than this; 0 = no cap. pymatgen holds a whole trajectory "
                        "in RAM, so on a batch job one huge output can get the whole job "
                        "cgroup-killed — set this for long unattended runs, sized to --mem.")
    p.add_argument("--max-records", type=int, default=None)


def _add_split(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("split", help="split a manifest into N parts for an array job")
    p.add_argument("--in", dest="in_path", required=True, help="manifest JSONL to split")
    p.add_argument("--parts", type=int, required=True, help="number of parts (array size)")
    p.add_argument("--out-dir", required=True, help="dir for <stem>.part-NNN.jsonl files")


def _add_merge(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("merge-datasets", help="fold per-task dataset dirs into one")
    p.add_argument("--into", required=True, help="destination dataset dir")
    p.add_argument("sources", nargs="+", help="per-task dataset dirs to merge in")


def _add_verify(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("verify", help="check metadata<->shard integrity + report dataset stats")
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))


def _add_purge_raw(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("purge-raw", help="delete raw extracted trees whose calcs are all parsed")
    p.add_argument("--raw-dir", default=str(config.RAW_DIR))
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))
    p.add_argument("--fetched", default=None,
                   help="fetched manifest (default: <raw-dir>/../manifests/fetched.jsonl)")
    p.add_argument("--dry-run", action="store_true", help="report only; delete nothing")


def _add_status(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("status", help="read-only snapshot of harvest progress "
                                      "(counts, staging vs quota, rejection reasons)")
    p.add_argument("--manifests-dir", default=str(config.MANIFEST_DIR))
    p.add_argument("--raw-dir", default=str(config.RAW_DIR))
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))
    p.add_argument("--keep", default=None,
                   help="keep-list for the fetch %% denominator (default: <manifests>/keep.jsonl)")
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="show staging bytes as %% of this budget (match the pipeline's value)")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="show staging inodes as %% of this file budget (match the pipeline's value)")
    p.add_argument("--json", action="store_true", help="machine-readable output instead of text")


def _add_pipeline(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("pipeline", help="stage 2-4 overlapped: fetch(i+1) || parse+purge(i), "
                                        "disk-paced (bounds peak staging under a quota)")
    p.add_argument("--in", dest="in_path", required=True, help="triaged keep-list JSONL")
    p.add_argument("--parts", type=int, required=True,
                   help="split the keep-list into this many batches (each fetched, parsed, "
                        "purged in turn; fetch of batch i+1 overlaps parse+purge of batch i)")
    p.add_argument("--raw-dir", default=str(config.RAW_DIR))
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))
    p.add_argument("--parts-dir", default=None,
                   help="dir for the split part manifests (default: <in>.pipeline_parts/)")
    p.add_argument("--max-bytes", type=int, default=0,
                   help="per-file download cap; 0 = no cap (default: uncapped for the harvest)")
    p.add_argument("--max-member-bytes", type=int, default=DEFAULT_MAX_MEMBER_BYTES,
                   help=f"cap on each extracted file; 0 = no cap (default {DEFAULT_MAX_MEMBER_BYTES // 10**9}GB)")
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="disk budget for the whole raw staging dir; 0 = no limit. It already "
                        "covers both concurrently-staged batches (the valve measures the whole "
                        "dir), so size it ~0.8*quota — 800000000000 for a 1 TB quota.")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="inode budget for the whole raw staging dir; 0 = no limit. On CSD3 "
                        "this binds before bytes (1M-file quota vs ~0.3 TB of small "
                        "extracted files) — suggest ~800000 for a 1M-file quota.")
    p.add_argument("--workers", type=int, default=4, help="concurrent downloads per batch (default 4)")
    p.add_argument("--no-zip-stream", dest="zip_stream", action="store_false",
                   help="disable targeted ZIP member fetch (on by default; pulls only the "
                        "VASP files out of a .zip over HTTP Range, falling back to a whole "
                        "download when a zip is not addressable this way)")
    p.add_argument("--zip-stream-max-files", type=int, default=DEFAULT_ZIP_STREAM_MAX_FILES,
                   help=f"max VASP members pulled individually from one .zip before falling "
                        f"back to a whole download (default {DEFAULT_ZIP_STREAM_MAX_FILES})")
    p.add_argument("--max-primary-bytes", type=int, default=0,
                   help="parse guard: skip any single vasprun.xml/vaspout.h5/OUTCAR larger "
                        "than this (0 = no cap). Recommended on a batch job so one huge "
                        "output cannot get the whole job cgroup-killed mid-harvest.")


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()  # pick up ZENODO_TOKEN / ZENODO_HARVEST_DATA from .env if present
    config.refresh_paths()  # so a .env ZENODO_HARVEST_DATA is honoured, not just a real export
    parser = argparse.ArgumentParser(prog="zenodo_harvest", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)
    _add_discover(sub)
    _add_triage(sub)
    _add_fetch(sub)
    _add_parse(sub)
    _add_split(sub)
    _add_merge(sub)
    _add_verify(sub)
    _add_purge_raw(sub)
    _add_status(sub)
    _add_pipeline(sub)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.cmd == "discover":
        client = ZenodoClient(base=args.base)
        summary = discover(
            client,
            queries=args.queries or DEFAULT_QUERIES,
            out_path=args.out,
            resource_types=args.resource_types or DEFAULT_RESOURCE_TYPES,
            exhaustive=args.exhaustive,
            max_records=args.max_records,
            fresh=args.fresh,
            license_gate=args.license_gate,
        )
    elif args.cmd == "triage":
        summary = triage(
            args.in_path, args.out_path,
            min_rank=args.min_rank, peek=args.peek,
            require_confirmed=args.require_confirmed,
        )
    elif args.cmd == "fetch":
        rejections = args.rejections or str(Path(args.raw_dir).parent / "manifests" / "rejections.jsonl")
        # 0 => no cap: max_bytes None disables the archive cap; member cap becomes ~unbounded.
        max_bytes = None if args.max_bytes == 0 else args.max_bytes
        max_member_bytes = args.max_member_bytes if args.max_member_bytes > 0 else (1 << 62)
        summary = fetch(
            args.in_path, out_path=args.out_path, raw_dir=args.raw_dir,
            rejections_path=rejections, max_bytes=max_bytes,
            max_records=args.max_records, retry_rejected=args.retry_rejected,
            max_member_bytes=max_member_bytes,
            max_disk_bytes=(args.max_disk_bytes or None),
            max_disk_files=(args.max_disk_files or None),
            workers=args.workers,
            zip_stream=args.zip_stream,
            zip_stream_max_files=args.zip_stream_max_files,
        )
    elif args.cmd == "parse":
        # Default the rejection log INSIDE the dataset dir, not a shared sibling: in the
        # array-job model every task writes its own <dataset-dir>/task-i, so a shared
        # parent/manifests/rejections.jsonl would take concurrent appends from N tasks
        # (interleaves/corrupts on Lustre/NFS). Per-dataset-dir keeps each task's log private.
        rejections = args.rejections or str(Path(args.dataset_dir) / "rejections.jsonl")
        try:
            summary = parse(
                args.in_path, dataset_dir=args.dataset_dir, rejections_path=rejections,
                frames_per_shard=args.frames_per_shard, max_records=args.max_records,
                raw_dir=args.raw_dir, gzip_level=args.gzip_level,
                max_primary_bytes=args.max_primary_bytes,
            )
        except DatasetLockError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    elif args.cmd == "split":
        summary = split_manifest(args.in_path, args.parts, args.out_dir)
    elif args.cmd == "merge-datasets":
        try:
            summary = merge_datasets(args.into, args.sources)
        except DatasetLockError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    elif args.cmd == "status":
        from .status import format_status, status_report
        report = status_report(
            manifests_dir=args.manifests_dir, raw_dir=args.raw_dir,
            dataset_dir=args.dataset_dir, keep_path=args.keep,
            max_disk_bytes=(args.max_disk_bytes or None),
            max_disk_files=(args.max_disk_files or None),
        )
        print(json.dumps(report, indent=2) if args.json else format_status(report))
        return 0  # read-only: always succeeds
    elif args.cmd == "verify":
        summary = verify_dataset(args.dataset_dir)
    elif args.cmd == "purge-raw":
        fetched = args.fetched or str(Path(args.raw_dir).parent / "manifests" / "fetched.jsonl")
        try:
            summary = purge_raw(args.raw_dir, args.dataset_dir, fetched=fetched,
                                dry_run=args.dry_run)
        except FileNotFoundError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
    elif args.cmd == "pipeline":
        from .pipeline import run_pipeline
        raw_dir, ds_dir = Path(args.raw_dir), Path(args.dataset_dir)
        parts_dir = Path(args.parts_dir or str(Path(args.in_path).with_suffix("")) + ".pipeline_parts")
        split_info = split_manifest(args.in_path, args.parts, parts_dir)
        part_paths = [Path(pw["path"]) for pw in split_info["parts_written"] if pw["lines"] > 0]
        max_bytes = None if args.max_bytes == 0 else args.max_bytes
        max_member_bytes = args.max_member_bytes if args.max_member_bytes > 0 else (1 << 62)
        max_disk_bytes = args.max_disk_bytes or None
        max_disk_files = args.max_disk_files or None
        fetch_rej = str(raw_dir.parent / "manifests" / "rejections.jsonl")
        parse_rej = str(ds_dir / "rejections.jsonl")

        def _fetched_path(part: Path) -> Path:
            return part.with_name(part.stem + ".fetched.jsonl")

        def fetch_fn(part: Path) -> bool:
            """Fetch one batch. Returns False if the disk-budget valve stopped it part
            way, so run_pipeline drains the background parse+purge and resumes THIS
            part instead of carrying on and silently dropping its remaining records."""
            summary = fetch(str(part), out_path=str(_fetched_path(part)),
                            raw_dir=str(raw_dir), rejections_path=fetch_rej,
                            max_bytes=max_bytes, max_member_bytes=max_member_bytes,
                            max_disk_bytes=max_disk_bytes, max_disk_files=max_disk_files,
                            workers=args.workers, zip_stream=args.zip_stream,
                            zip_stream_max_files=args.zip_stream_max_files)
            return not summary.get("stopped_disk_budget", False)

        def process_fn(part: Path) -> None:
            fetched = str(_fetched_path(part))
            parse(fetched, dataset_dir=str(ds_dir), rejections_path=parse_rej,
                  raw_dir=str(raw_dir), max_primary_bytes=args.max_primary_bytes)
            purge_raw(str(raw_dir), str(ds_dir), fetched=fetched)

        fetch_error: str | None = None
        done_parts: list[Path] = []
        errors: list[tuple[Path, Exception]] = []
        try:
            done_parts, errors = run_pipeline(part_paths, fetch_fn, process_fn,
                                              after_workers=1)
        except DatasetLockError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        except Exception as exc:
            # A foreground fetch failed hard (systemic: auth, DNS, a full filesystem).
            # Don't lose the run's report — record it, still verify what did land, and
            # exit non-zero. Everything staged/parsed so far is resumable.
            logging.getLogger(__name__).exception("pipeline fetch failed")
            fetch_error = f"{type(exc).__name__}: {exc}"
        verify = verify_dataset(str(ds_dir))
        summary = {
            "parts": len(part_paths),
            "parts_done": len(done_parts),
            "fetch_error": fetch_error,
            "process_errors": [{"part": str(p), "error": str(e)} for p, e in errors],
            "verify": verify,
            "ok": not errors and fetch_error is None and verify.get("ok", True),
        }
    else:  # pragma: no cover
        parser.error(f"unknown command {args.cmd}")

    print(json.dumps(summary, indent=2))
    return 0 if summary.get("ok", True) else 1


if __name__ == "__main__":
    sys.exit(main())
