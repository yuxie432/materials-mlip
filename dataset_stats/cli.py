"""Command line for the dataset statistics.

    python -m dataset_stats.cli meta      --source nomad --workers 16   # metadata.jsonl -> rows
    python -m dataset_stats.cli scan      --source nomad --workers 32   # shards -> frame rows
    python -m dataset_stats.cli ref-fetch --ref mptrj                   # download a reference set
    python -m dataset_stats.cli ref-scan  --ref mptrj --workers 16      # reference -> same rows
    python -m dataset_stats.cli report    --refs mptrj omat24_val ...   # -> report.json + .md

Paths default to the CSD3 layout: dataset dirs ``$ZENODO_HARVEST_DATA/dataset``,
``$NOMAD_HARVEST_DATA/dataset``, ``$MC_HARVEST_DATA/dataset`` (NOMAD / Materials Cloud roots
default to siblings of an absolute Zenodo root, as their own CLIs do), outputs under
``$DATASET_STATS_DATA`` (default a ``stats`` sibling). Everything is read-only on the datasets.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from zenodo_harvest import config

from .common import SOURCES

_ENV = {"zenodo": "ZENODO_HARVEST_DATA", "nomad": "NOMAD_HARVEST_DATA",
        "materials_cloud": "MC_HARVEST_DATA"}


def source_root(source: str) -> Path:
    env = os.environ.get(_ENV[source])
    if env:
        return Path(env)
    if source == "zenodo":
        return config.DATA_ROOT
    base = config.DATA_ROOT
    return base.parent / source if base.is_absolute() else base / source


def stats_root() -> Path:
    env = os.environ.get("DATASET_STATS_DATA")
    if env:
        return Path(env)
    base = config.DATA_ROOT
    return base.parent / "stats" if base.is_absolute() else base / "stats"


def _dataset_dir(args: argparse.Namespace) -> Path:
    return Path(args.dataset_dir) if args.dataset_dir else source_root(args.source) / "dataset"


def cmd_meta(args: argparse.Namespace) -> int:
    from .meta import meta_dataset
    meta = Path(args.metadata) if args.metadata else _dataset_dir(args) / "metadata.jsonl"
    out = Path(args.stats_root) / args.source / "meta"
    print(json.dumps(meta_dataset(meta, out, workers=args.workers, parts=args.parts), indent=1))
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    from .scan import scan_dataset
    out = Path(args.stats_root) / args.source / "scan"
    include, finfo = None, None
    if args.individual_only:
        from .meta import individual_include
        include, finfo = individual_include(Path(args.stats_root) / args.source / "meta")
        logging.getLogger(__name__).info("individual uploads only: %s", finfo)
    res = scan_dataset(_dataset_dir(args), out, workers=args.workers, limit=args.limit,
                       force=args.force, describe=not args.no_describe, include=include,
                       filter_info=finfo)
    print(json.dumps(res, indent=1))
    return 0 if not res["shards_failed"] else 1


def _refs_root(args: argparse.Namespace) -> Path:
    return Path(args.refs_root) if args.refs_root else Path(args.stats_root) / "refs"


def cmd_ref_fetch(args: argparse.Namespace) -> int:
    from .reference import fetch_reference
    for ref in args.ref:
        print(json.dumps(fetch_reference(ref, _refs_root(args))))
    return 0


def cmd_ref_scan(args: argparse.Namespace) -> int:
    from .reference import scan_reference
    rc = 0
    for ref in args.ref:
        res = scan_reference(ref, _refs_root(args), args.stats_root, workers=args.workers,
                             force=args.force)
        print(json.dumps(res, indent=1))
        rc |= 1 if res["failed"] else 0
    return rc


def cmd_report(args: argparse.Namespace) -> int:
    from .render import render_markdown
    from .report import build_report
    srcs = args.sources or list(SOURCES)
    dirs = {s: source_root(s) / "dataset" for s in srcs}
    for spec in args.dataset_dir or []:
        k, _, v = spec.partition("=")
        dirs[k] = Path(v)
    rep, _internals = build_report(args.stats_root, srcs, dirs, references=args.refs or [],
                                   refs_root=_refs_root(args),
                                   individual_only=args.individual_only)
    out = Path(args.out) if args.out else Path(args.stats_root) / "report"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "report.json", "w") as fh:
        json.dump(rep, fh, indent=1, default=str)
    (out / "report.md").write_text(render_markdown(rep))
    print(f"wrote {out / 'report.json'} and {out / 'report.md'}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m dataset_stats.cli", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stats-root", default=str(stats_root()),
                   help="output root (default $DATASET_STATS_DATA or a 'stats' sibling)")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("meta", help="metadata.jsonl -> per-calc rows (fast, re-run freely)")
    m.add_argument("--source", choices=SOURCES, required=True)
    m.add_argument("--dataset-dir", help="default <source root>/dataset")
    m.add_argument("--metadata", help="explicit metadata.jsonl (default <dataset-dir>/metadata.jsonl)")
    m.add_argument("--workers", type=int, default=1)
    m.add_argument("--parts", type=int, help="byte-range chunks (default 4 x workers)")
    m.set_defaults(func=cmd_meta)

    s = sub.add_parser("scan", help="every shard -> frame rows + per-calc structures (resumable)")
    s.add_argument("--source", choices=SOURCES, required=True)
    s.add_argument("--dataset-dir", help="default <source root>/dataset")
    s.add_argument("--workers", type=int, default=1)
    s.add_argument("--limit", type=int, help="scan at most N (not yet scanned) shards")
    s.add_argument("--force", action="store_true", help="re-scan shards already done")
    s.add_argument("--no-describe", action="store_true",
                   help="skip the per-calc structure descriptors (spglib, neighbour list)")
    s.add_argument("--individual-only", action="store_true",
                   help="scan only individual uploads: skip calcs of institutional high-"
                        "throughput origin (NOMAD's Alexandria-group uploads); needs `meta` first")
    s.set_defaults(func=cmd_scan)

    from .reference import REFERENCES
    for cmd, fn, hlp in (("ref-fetch", cmd_ref_fetch, "download reference datasets"),
                         ("ref-scan", cmd_ref_scan, "scan downloaded reference datasets")):
        rp = sub.add_parser(cmd, help=hlp)
        rp.add_argument("--ref", nargs="+", choices=sorted(REFERENCES), required=True)
        rp.add_argument("--refs-root", help="download root (default <stats-root>/refs)")
        if cmd == "ref-scan":
            rp.add_argument("--workers", type=int, default=1)
            rp.add_argument("--force", action="store_true")
        rp.set_defaults(func=fn)

    r = sub.add_parser("report", help="combine meta + scan outputs -> report.json + report.md")
    r.add_argument("--sources", nargs="*", choices=SOURCES)
    r.add_argument("--refs", nargs="*", default=[], choices=sorted(REFERENCES),
                   help="scanned reference datasets to compare against")
    r.add_argument("--refs-root", help="reference downloads (default <stats-root>/refs; the MP "
                                       "elemental references found there add a formation-energy "
                                       "proxy)")
    r.add_argument("--individual-only", action="store_true",
                   help="report individual uploads only (implied for a source scanned with "
                        "--individual-only)")
    r.add_argument("--dataset-dir", action="append",
                   help="SOURCE=PATH, only to size the shards on disk (repeatable)")
    r.add_argument("--out", help="default <stats-root>/report")
    r.set_defaults(func=cmd_report)
    return p


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    config.refresh_paths()
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
