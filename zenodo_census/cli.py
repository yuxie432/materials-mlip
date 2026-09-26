"""Command line for the Zenodo census (find the VASP data keyword discovery misses).

    python -m zenodo_census.cli census            # ~3.4 h: every archive-bearing record -> census.jsonl
    python -m zenodo_census.cli links             # DataCite paper->Zenodo refs + Europe PMC mentions
    python -m zenodo_census.cli resolve           # cited VERSION ids -> concepts (~250 searches)
    python -m zenodo_census.cli openalex          # does each linked/citing paper cite VASP? (cached)
    python -m zenodo_census.cli score             # offline tiers -> scored.jsonl + score_report.json
    python -m zenodo_census.cli triage            # peeks -> census_keep.jsonl (+ licence_review.jsonl)
    python -m zenodo_census.cli status            # progress of each stage (read-only)
    # then the ordinary Zenodo pipeline, straight into the production dataset:
    IN=<census root>/census_keep.jsonl RAW_DIR=<zenodo root>/raw_census sbatch scripts/csd3/20_pipeline.sh

Everything lives under ``$ZENODO_CENSUS_DATA`` (default ``<ZENODO_HARVEST_DATA>/census``). The
keyword harvest's manifests (candidates / keep lists) and the dataset's ``metadata.jsonl`` are read
from the Zenodo tree to exclude what is already evaluated or harvested.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from zenodo_harvest import config

from .census import CensusClient, census_keys, run_census
from .headpeek import DEFAULT_HEAD_BYTES
from .links import (
    datacite_references,
    epmc_mentions,
    linked_ids,
    load_citing,
    load_epmc,
    load_openalex,
    load_versions,
    openalex_lookup,
    resolve_versions,
)
from .score import Exclusions, build_seeds, lookup_dois, score
from .signals import Seeds
from .triage import (
    DEFAULT_INTERVAL,
    DEFAULT_NEGATIVE_SAMPLE,
    DEFAULT_RESIDUAL_SAMPLE,
    triage,
)

# keyword-harvest manifests whose records were already evaluated (their verdicts stand).
# NOT candidates_nolicense.jsonl: it also holds open records created after the keyword discover
# (2026-07-30) that the NC filter dropped unevaluated — the census must look at those.
DEFAULT_EXCLUDE_MANIFESTS = ("candidates_full.jsonl", "keep.jsonl", "nc_candidates.jsonl",
                             "nc_keep.jsonl", "byid_10579527_keep.jsonl")


def census_root() -> Path:
    env = os.environ.get("ZENODO_CENSUS_DATA")
    return Path(env) if env else config.DATA_ROOT / "census"


def _paths() -> dict[str, Path]:
    r = census_root()
    return {"root": r, "census": r / "census.jsonl", "links": r / "links",
            "datacite": r / "links" / "datacite_refs.jsonl",
            "epmc": r / "links" / "epmc_mentions.jsonl",
            "openalex": r / "links" / "openalex.jsonl",
            "versions": r / "links" / "zenodo_versions.jsonl",
            "scored": r / "scored.jsonl", "keep": r / "census_keep.jsonl"}


def _default_seed_metadata() -> list[str]:
    """The Materials Cloud dataset's metadata.jsonl (its creators are extra seed names), wherever
    its adapter keeps it: ``$MC_HARVEST_DATA``, a ``materials_cloud`` sibling of an absolute Zenodo
    root, or ``<relative data root>/materials_cloud``."""
    env = os.environ.get("MC_HARVEST_DATA")
    root = Path(env) if env else (config.DATA_ROOT.parent / "materials_cloud"
                                  if config.DATA_ROOT.is_absolute()
                                  else config.DATA_ROOT / "materials_cloud")
    p = root / "dataset" / "metadata.jsonl"
    return [str(p)] if p.is_file() else []


def _context(args: argparse.Namespace) -> tuple[Exclusions, Seeds]:
    excl = Exclusions()
    meta = Path(args.dataset_metadata)
    manifests = args.exclude_manifest if args.exclude_manifest is not None else [
        str(config.MANIFEST_DIR / m) for m in DEFAULT_EXCLUDE_MANIFESTS]
    present = [m for m in manifests if Path(m).is_file()]
    # Without the dataset / keyword manifests nothing is excluded and records already harvested
    # would re-enter the production dataset under new calc_ids — refuse unless told otherwise.
    if not args.allow_missing_exclusions and (not meta.is_file() or not present):
        raise SystemExit(f"exclusion inputs missing (dataset metadata {meta}: "
                         f"{'ok' if meta.is_file() else 'MISSING'}; manifests found: "
                         f"{present or 'NONE'}) — check ZENODO_HARVEST_DATA, or pass "
                         "--allow-missing-exclusions")
    if meta.is_file():
        excl.add_dataset(meta)
    for m in manifests:
        if Path(m).is_file():
            n = excl.add_manifest(m)
            logging.info("excluding %d records evaluated in %s", n, m)
        else:
            logging.warning("exclusion manifest %s not found", m)
    logging.info("re-checking %d keyword candidates the old triage could not examine",
                 len(excl.recheck))
    seeds_meta = args.seed_metadata if args.seed_metadata is not None else _default_seed_metadata()
    seeds = build_seeds(args.census, excl, seeds_meta)
    return excl, seeds


def _common(p: argparse.ArgumentParser) -> None:
    P = _paths()
    p.add_argument("--census", default=str(P["census"]))
    p.add_argument("--dataset-metadata", default=str(config.DATASET_DIR / "metadata.jsonl"),
                   help="the Zenodo VASP dataset's metadata.jsonl (seeds + exclusions)")
    p.add_argument("--exclude-manifest", action="append", default=None,
                   help="keyword-harvest manifest(s) of already-evaluated records (repeatable; "
                        f"default: {', '.join(DEFAULT_EXCLUDE_MANIFESTS)} in the manifests dir)")
    p.add_argument("--seed-metadata", action="append", default=None,
                   help="extra dataset metadata.jsonl whose creators seed the name signal "
                        "(default: the Materials Cloud dataset, if present)")
    p.add_argument("--allow-missing-exclusions", action="store_true",
                   help="run even without the dataset metadata / keyword manifests (tests)")


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    config.refresh_paths()
    P = _paths()
    ap = argparse.ArgumentParser(prog="zenodo_census", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("census", help="enumerate every archive-bearing Zenodo record")
    c.add_argument("--out", default=str(P["census"]))
    c.add_argument("--start", default="2013-01-01")
    c.add_argument("--end", default=None, help="last created day (default: today)")
    c.add_argument("--page-size", type=int, default=None)
    c.add_argument("--max-records", type=int, default=None)
    c.add_argument("--fresh", action="store_true")

    li = sub.add_parser("links", help="DataCite paper->Zenodo references + Europe PMC mentions")
    li.add_argument("--no-datacite", action="store_true")
    li.add_argument("--no-epmc", action="store_true")
    li.add_argument("--max-pages", type=int, default=None)

    rs = sub.add_parser("resolve", help="map linked Zenodo version ids to their concepts")
    rs.add_argument("--census", default=str(P["census"]))

    oa = sub.add_parser("openalex", help="look up the papers linked to / citing T2/T3 records")
    _common(oa)
    oa.add_argument("--workers", type=int, default=4)
    oa.add_argument("--interval", type=float, default=0.125)
    oa.add_argument("--max-lookups", type=int, default=None)
    oa.add_argument("--include-t1", action="store_true")

    sc = sub.add_parser("score", help="tier every census record (offline)")
    _common(sc)
    sc.add_argument("--out", default=str(P["scored"]))

    tr = sub.add_parser("triage", help="peek the selected records' archives -> keep-list")
    tr.add_argument("--scored", default=str(P["scored"]))
    tr.add_argument("--census", default=str(P["census"]))
    tr.add_argument("--out", default=str(P["keep"]))
    tr.add_argument("--tiers", nargs="+", default=["T1", "T2"])
    tr.add_argument("--types", nargs="+", default=None,
                    help="resource types for a full T3 run / the residual sample "
                         "(default: all but software)")
    tr.add_argument("--residual-sample", type=int, default=DEFAULT_RESIDUAL_SAMPLE)
    tr.add_argument("--negative-sample", type=int, default=DEFAULT_NEGATIVE_SAMPLE)
    tr.add_argument("--seed", type=int, default=20260925)
    tr.add_argument("--interval", type=float, default=DEFAULT_INTERVAL,
                    help="seconds between request starts (0.8 = 75/min, 4.5k/h)")
    tr.add_argument("--peek-workers", type=int, default=3)
    tr.add_argument("--head-bytes", type=int, default=DEFAULT_HEAD_BYTES)
    tr.add_argument("--max-records", type=int, default=None)
    tr.add_argument("--exclude-keep", nargs="*", default=[],
                    help="earlier census keep-lists whose records this run must skip")

    sub.add_parser("status", help="progress of each census stage")

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.cmd != "status":                     # status is read-only
        P["root"].mkdir(parents=True, exist_ok=True)
        P["links"].mkdir(parents=True, exist_ok=True)

    if args.cmd == "census":
        from datetime import date
        client = CensusClient()
        if not client.token:
            logging.warning("no ZENODO_TOKEN: pages of 25 (4x the requests)")
        out = run_census(client, args.out, start=date.fromisoformat(args.start),
                         end=date.fromisoformat(args.end) if args.end else None,
                         page_size=args.page_size, max_records=args.max_records,
                         fresh=args.fresh)
    elif args.cmd == "links":
        # each channel runs whatever happens to the other; a failure is reported, not hidden
        out = {}
        failed = []
        jobs = [] if args.no_datacite else [("datacite", lambda: datacite_references(
            P["datacite"], max_pages=args.max_pages))]
        jobs += [] if args.no_epmc else [("epmc", lambda: epmc_mentions(P["epmc"]))]
        for name, job in jobs:
            try:
                out[name] = job()
            except Exception as exc:  # noqa: BLE001 - reported, then the next channel runs
                logging.error("links: %s failed: %s", name, exc)
                out[name] = {"error": f"{type(exc).__name__}: {exc}"}
                failed.append(name)
        if failed:
            print(json.dumps(out, indent=1, default=str))
            return 1
    elif args.cmd == "resolve":
        keys = census_keys(args.census)
        ids = {i for i in linked_ids(P["datacite"], P["epmc"]) if i not in keys}
        out = resolve_versions(ids, P["versions"], CensusClient())
    elif args.cmd == "openalex":
        excl, seeds = _context(args)
        versions = load_versions(P["versions"])
        dois = lookup_dois(args.census, seeds, excl, load_citing(P["datacite"], versions),
                           include_t1=args.include_t1, epmc=load_epmc(P["epmc"], versions))
        logging.info("openalex: %d candidate paper DOIs", len(dois))
        out = openalex_lookup(dois, P["openalex"], workers=args.workers, interval=args.interval,
                              max_lookups=args.max_lookups)
    elif args.cmd == "score":
        excl, seeds = _context(args)
        versions = load_versions(P["versions"])
        out = score(args.census, args.out, excl=excl, seeds=seeds,
                    citing=load_citing(P["datacite"], versions),
                    epmc=load_epmc(P["epmc"], versions),
                    openalex=load_openalex(P["openalex"]), versions=versions)
    elif args.cmd == "triage":
        out = triage(args.scored, args.census, args.out, tiers=args.tiers, types=args.types,
                     residual_sample=args.residual_sample, negative_sample=args.negative_sample,
                     seed=args.seed, token=os.environ.get("ZENODO_TOKEN"),
                     interval=args.interval, peek_workers=args.peek_workers,
                     head_bytes=args.head_bytes, max_records=args.max_records,
                     exclude_keep=args.exclude_keep)
    else:  # status
        out = status(P)
    print(json.dumps(out, indent=1, default=str))
    return 0


def _lines(p: Path) -> int:
    if not p.is_file():
        return 0
    with p.open("rb") as fh:
        return sum(1 for _ in fh)


def _poison_summary(p: Path) -> dict[str, int]:
    """Distinct records in the census poison log by outcome (a re-paged window logs a record again;
    a torn line from a live writer is skipped)."""
    seen: dict[str, set[str]] = {}
    if p.is_file():
        with p.open() as fh:
            for line in fh:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = str(e.get("id") or f"{e.get('query')}@{e.get('offset')}")
                seen.setdefault(str(e.get("status")), set()).add(key)
    return {k: len(v) for k, v in sorted(seen.items())}


def status(P: dict[str, Path]) -> dict[str, object]:
    """Line counts / completion flags of each stage's output (read-only; safe while running)."""
    cur = Path(str(P["datacite"]) + ".cursor")
    rep = P["root"] / "score_report.json"
    trep = P["keep"].with_name(P["keep"].stem + ".report.json")
    return {
        "root": str(P["root"]),
        "census_lines": _lines(P["census"]),
        "census_windows_done": _lines(Path(str(P["census"]) + ".windows.jsonl")),
        "census_poison": _poison_summary(Path(str(P["census"]) + ".poison.jsonl")),
        "datacite_links": _lines(P["datacite"]),
        "datacite_complete": cur.is_file() and cur.read_text().strip() == "done",
        "epmc_papers": _lines(P["epmc"]),
        "versions_resolved": _lines(P["versions"]),
        "openalex_cached": _lines(P["openalex"]),
        "scored": _lines(P["scored"]),
        "score_counts": json.loads(rep.read_text()).get("counts") if rep.is_file() else None,
        "keep_records": _lines(P["keep"]),
        "triage_kept": json.loads(trep.read_text())["summary"].get("kept")
        if trep.is_file() else None,
        "licence_review": _lines(P["keep"].with_name(P["keep"].stem + ".licence_review.jsonl")),
        "peeks_cached": _lines(P["root"] / "peeks.jsonl"),
    }


if __name__ == "__main__":
    sys.exit(main())
