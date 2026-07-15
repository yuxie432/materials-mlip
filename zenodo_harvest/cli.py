"""Command-line entrypoints for the harvest pipeline.

Examples
--------
Small WSL trial (first 10k per query, no download)::

    python -m zenodo_harvest.cli discover --max-records 200 \
        --out data/manifests/candidates.jsonl
    python -m zenodo_harvest.cli triage \
        --in data/manifests/candidates.jsonl \
        --out data/manifests/keep.jsonl --min-rank 3

Confirm archive contents without downloading (reads zip central directory)::

    python -m zenodo_harvest.cli triage --in data/manifests/candidates.jsonl \
        --out data/manifests/keep.jsonl --peek

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
from .fetch import fetch
from .parse import parse
from .store import DatasetLockError
from .triage import triage


def _add_discover(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("discover", help="stage 0: build candidate manifest from Zenodo search")
    p.add_argument("--out", default="data/manifests/candidates.jsonl")
    p.add_argument("--query", action="append", dest="queries",
                   help="override default queries (repeatable)")
    p.add_argument("--resource-type", action="append", dest="resource_types",
                   default=None, help="resource type(s) to keep (default: dataset)")
    p.add_argument("--exhaustive", action="store_true",
                   help="recursive date-partitioning to exceed the 10k window (cluster)")
    p.add_argument("--fresh", action="store_true",
                   help="ignore + remove any <out>.hits.jsonl checkpoint (clean rebuild)")
    p.add_argument("--max-records", type=int, default=None)
    p.add_argument("--base", default="https://zenodo.org")


def _add_triage(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("triage", help="stage 1: filter candidates to a keep-list")
    p.add_argument("--in", dest="in_path", required=True)
    p.add_argument("--out", dest="out_path", required=True)
    p.add_argument("--min-rank", type=int, default=3,
                   help="4=vasp_direct 3=+archive 2=+processed")
    p.add_argument("--peek", action="store_true",
                   help="inspect remote zip central directories (no full download)")
    p.add_argument("--require-confirmed", action="store_true",
                   help="drop archive records that peek could not confirm")


def _add_fetch(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("fetch", help="stage 2: download + stage VASP files for a keep-list")
    p.add_argument("--in", dest="in_path", required=True, help="triaged keep-list JSONL")
    p.add_argument("--out", dest="out_path", default=str(config.MANIFEST_DIR / "fetched.jsonl"))
    p.add_argument("--raw-dir", default=str(config.RAW_DIR))
    p.add_argument("--rejections", default=None,
                   help="rejection log path (default: <raw-dir>/../manifests/rejections.jsonl)")
    p.add_argument("--max-bytes", type=int, default=500_000_000,
                   help="skip any file/archive larger than this (default 500MB)")
    p.add_argument("--retry-rejected", action="store_true",
                   help="reprocess records previously rejected as terminal (e.g. after raising --max-bytes)")
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


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()  # pick up ZENODO_TOKEN from .env if present
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
        )
    elif args.cmd == "triage":
        summary = triage(
            args.in_path, args.out_path,
            min_rank=args.min_rank, peek=args.peek,
            require_confirmed=args.require_confirmed,
        )
    elif args.cmd == "fetch":
        rejections = args.rejections or str(Path(args.raw_dir).parent / "manifests" / "rejections.jsonl")
        summary = fetch(
            args.in_path, out_path=args.out_path, raw_dir=args.raw_dir,
            rejections_path=rejections, max_bytes=args.max_bytes,
            max_records=args.max_records, retry_rejected=args.retry_rejected,
        )
    elif args.cmd == "parse":
        rejections = args.rejections or str(Path(args.dataset_dir).parent / "manifests" / "rejections.jsonl")
        try:
            summary = parse(
                args.in_path, dataset_dir=args.dataset_dir, rejections_path=rejections,
                frames_per_shard=args.frames_per_shard, max_records=args.max_records,
                raw_dir=args.raw_dir,
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
    else:  # pragma: no cover
        parser.error(f"unknown command {args.cmd}")

    print(json.dumps(summary, indent=2))
    return 0 if summary.get("ok", True) else 1


if __name__ == "__main__":
    sys.exit(main())
