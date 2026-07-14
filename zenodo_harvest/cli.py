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
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from . import config
from .client import ZenodoClient
from .discover import DEFAULT_QUERIES, discover
from .fetch import fetch
from .parse import parse
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
    p.add_argument("--max-bytes", type=int, default=500_000_000,
                   help="skip any file/archive larger than this (default 500MB)")
    p.add_argument("--max-records", type=int, default=None)


def _add_parse(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("parse", help="stage 3: parse fetched calcs -> extxyz.gz + metadata.jsonl")
    p.add_argument("--in", dest="in_path", required=True, help="fetched manifest JSONL")
    p.add_argument("--dataset-dir", default=str(config.DATASET_DIR))
    p.add_argument("--frames-per-shard", type=int, default=10_000)
    p.add_argument("--max-records", type=int, default=None)


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
            resource_types=args.resource_types or ("dataset",),
            exhaustive=args.exhaustive,
            max_records=args.max_records,
        )
    elif args.cmd == "triage":
        summary = triage(
            args.in_path, args.out_path,
            min_rank=args.min_rank, peek=args.peek,
            require_confirmed=args.require_confirmed,
        )
    elif args.cmd == "fetch":
        summary = fetch(
            args.in_path, out_path=args.out_path, raw_dir=args.raw_dir,
            max_bytes=args.max_bytes, max_records=args.max_records,
        )
    elif args.cmd == "parse":
        summary = parse(
            args.in_path, dataset_dir=args.dataset_dir,
            frames_per_shard=args.frames_per_shard, max_records=args.max_records,
        )
    else:  # pragma: no cover
        parser.error(f"unknown command {args.cmd}")

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
