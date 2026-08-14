"""Command-line entrypoints for the NOMAD harvest.

Stages 0-2 (discover / fetch / the overlapped pipeline) are NOMAD-specific and live here;
stages 3-5 (parse / store / merge / verify) are the SHARED, unmodified ``zenodo_harvest``
code, so the NOMAD dataset comes out schema-identical to the Zenodo one::

    python -m nomad_harvest.cli smoke -n 12                       # Phase-0 validation (isolated)
    python -m nomad_harvest.cli discover --max-entries 200000 \
        --out data/manifests/nomad_keep.jsonl                     # keyset scan + gate + dedup

    # One overlapped, disk-paced command (fetch batch i+1 while parse+purge batch i), the
    # recommended shape for a long CSD3 job — reuses the shared parse/store/verify + valve:
    python -m nomad_harvest.cli pipeline --in data/manifests/nomad_keep.jsonl \
        --parts 40 --workers 4 --dataset-dir data/dataset/nomad \
        --max-disk-bytes 800000000000 --max-disk-files 800000

    # ...or the stages by hand (fetch, then the shared parser/verifier):
    python -m nomad_harvest.cli fetch --in data/manifests/nomad_keep.jsonl \
        --out data/manifests/nomad_fetched.jsonl
    python -m zenodo_harvest.cli parse  --in data/manifests/nomad_fetched.jsonl \
        --dataset-dir data/dataset/nomad
    python -m zenodo_harvest.cli verify --dataset-dir data/dataset/nomad
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from zenodo_harvest import config
from zenodo_harvest.dataset_ops import purge_raw, split_manifest, verify_dataset
from zenodo_harvest.parse import parse
from zenodo_harvest.store import DatasetLockError

from .client import NomadClient, direct_upload_vasp_query
from .harvest import discover_candidates, fetch_candidates, fetch_candidates_bulk


def _add_disk_valve(p: argparse.ArgumentParser) -> None:
    """Shared disk/inode-valve + worker flags (fetch and pipeline both pace with these)."""
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="stop staging cleanly once raw/ reaches this many bytes (0 = no "
                        "limit); ~0.8x the CSD3 1 TB hpc-work quota")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="stop staging once raw/ reaches this many inodes (0 = no limit); "
                        "the CSD3 1M-file quota binds first, so ~800000")
    p.add_argument("--workers", type=int, default=1,
                   help="concurrent downloads (NOMAD allows ~10; the raw endpoint is "
                        "latency-bound, so ~4-8 helps). Discovery is single-stream by design.")
    p.add_argument("--want-outcar", action="store_true",
                   help="also fetch OUTCAR when present (per-atom charges/spins on the final "
                        "frame; larger transfer — vasprun-only is the default)")
    p.add_argument("--per-entry", action="store_true",
                   help="use the per-entry fetch (2 requests/entry) instead of the DEFAULT "
                        "bulk fetch (~2 requests per --batch-size entries). Bulk is ~100x "
                        "fewer requests — the difference between a ~months and a ~days run; "
                        "use per-entry only to debug or if bulk raw/query is unavailable.")
    p.add_argument("--batch-size", type=int, default=300,
                   help="entries per bulk raw/query zip (bulk mode; bounds zip size + the "
                        "server's zip-assembly time)")
    p.add_argument("--tmp-dir", default=None,
                   help="node-local dir for transient batch zips (bulk mode; default a temp "
                        "dir under $TMPDIR). Keep it OFF the rds quota — e.g. CSD3 /local.")


def _fetch(client: NomadClient, in_path: str, out_path: str | None, raw_dir: str,
           args: argparse.Namespace, max_records: int | None) -> dict:
    """Run the selected fetch (bulk by default, per-entry with ``--per-entry``)."""
    kw = dict(raw_dir=raw_dir, out_path=out_path, want_outcar=args.want_outcar,
              max_records=max_records, max_disk_bytes=(args.max_disk_bytes or None),
              max_disk_files=(args.max_disk_files or None), workers=args.workers)
    if args.per_entry:
        return fetch_candidates(client, in_path, **kw)
    return fetch_candidates_bulk(client, in_path, batch_size=args.batch_size,
                                 tmp_dir=args.tmp_dir, **kw)


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    config.refresh_paths()  # honour ZENODO_HARVEST_DATA from .env (write to scratch, not /home)
    p = argparse.ArgumentParser(prog="nomad_harvest", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("discover", help="stage 0/1: keyset scan -> license-gated, "
                                        "Zenodo-deduped keep-list")
    d.add_argument("--out", default=str(config.MANIFEST_DIR / "nomad_keep.jsonl"))
    d.add_argument("--elements", nargs="+", default=None,
                   help="restrict to materials containing ALL these elements")
    d.add_argument("--max-entries", type=int, default=None,
                   help="cap the keep-list size (the bounded-sample scope; a keyset scan "
                        "spreads across uploads, so a cap is a diverse sample)")
    d.add_argument("--page-size", type=int, default=1000)
    d.add_argument("--zenodo-metadata", default=str(config.DATASET_DIR / "metadata.jsonl"),
                   help="Zenodo dataset metadata.jsonl to dedup against (skipped if absent)")
    d.add_argument("--no-license-gate", dest="license_gate", action="store_false")

    f = sub.add_parser("fetch", help="stage 2: stage each candidate's vasprun -> fetched.jsonl")
    f.add_argument("--in", dest="in_path", required=True, help="keep-list JSONL from discover")
    f.add_argument("--out", default=None,
                   help="fetched manifest (default: <raw-dir>/../manifests/nomad_fetched.jsonl)")
    f.add_argument("--raw-dir", default=str(config.RAW_DIR))
    f.add_argument("--max-records", type=int, default=None,
                   help="cap entries newly staged THIS run (resumed skips don't count)")
    _add_disk_valve(f)

    pi = sub.add_parser("pipeline", help="stages 2-4 overlapped: fetch(i+1) || parse+purge(i) "
                                         "+ verify, paced by the disk/inode valve")
    pi.add_argument("--in", dest="in_path", required=True, help="keep-list JSONL from discover")
    pi.add_argument("--parts", type=int, required=True,
                    help="split the keep-list into this many batches (each fetched, parsed, "
                         "purged in turn; fetch of batch i+1 overlaps parse+purge of batch i)")
    pi.add_argument("--parts-dir", default=None,
                    help="dir for the split part manifests (default: <in>.pipeline_parts/)")
    pi.add_argument("--raw-dir", default=str(config.RAW_DIR))
    pi.add_argument("--dataset-dir", default=str(config.DATASET_DIR / "nomad"),
                    help="dataset dir for the NOMAD shards + metadata (kept separate from the "
                         "Zenodo dataset; merge-datasets folds them together later)")
    pi.add_argument("--max-primary-bytes", type=int, default=2_000_000_000,
                    help="RAM guard: refuse to parse a primary bigger than this (0 = off). "
                         "pymatgen peak RSS ~10x file size; size to the job's --mem.")
    pi.add_argument("--parse-timeout", type=int, default=1200,
                    help="hard-kill a single calc's parse after N seconds (0 = off), so one "
                         "non-terminating pymatgen/ASE parse can't freeze the pipeline")
    _add_disk_valve(pi)

    s = sub.add_parser("smoke", help="Phase-0 live end-to-end validation (isolated temp dir)")
    s.add_argument("-n", type=int, default=12)
    s.add_argument("--elements", nargs="+", default=None)
    s.add_argument("--work-dir", default=None)
    s.add_argument("--keep", action="store_true")

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "smoke":
        from .smoke import run
        import tempfile
        work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="nomad_smoke_"))
        return run(args.n, args.elements, work, args.keep)

    client = NomadClient()
    if args.cmd == "discover":
        zmeta = args.zenodo_metadata if Path(args.zenodo_metadata).is_file() else None
        summary = discover_candidates(
            client, args.out, query=direct_upload_vasp_query(elements=args.elements),
            max_entries=args.max_entries, page_size=args.page_size,
            license_gate=args.license_gate, zenodo_metadata=zmeta)
    elif args.cmd == "fetch":
        summary = _fetch(client, args.in_path, args.out, args.raw_dir, args, args.max_records)
    elif args.cmd == "pipeline":
        return _run_pipeline(client, args)
    else:  # pragma: no cover
        p.error(f"unknown command {args.cmd}")

    print(json.dumps(summary, indent=2))
    return 0


def _run_pipeline(client: NomadClient, args: argparse.Namespace) -> int:
    """Overlapped fetch || parse+purge over the keep-list, ending with verify.

    Reuses the shared, I/O-agnostic ``zenodo_harvest.pipeline.run_pipeline`` (fetch batch
    i+1 while parse+purge batch i), the shared ``parse``/``purge_raw``/``verify_dataset``,
    and the NOMAD fetch's disk/inode valve. ``fetch_fn`` returns False when the valve stops
    a batch part-way, so run_pipeline drains the background parse+purge to reclaim staging
    and resumes THAT batch — a partly-fetched batch is never carried on and silently dropped.
    """
    from zenodo_harvest.pipeline import run_pipeline

    raw_dir, ds_dir = Path(args.raw_dir), Path(args.dataset_dir)
    parts_dir = Path(args.parts_dir or str(Path(args.in_path).with_suffix("")) + ".pipeline_parts")
    split_info = split_manifest(args.in_path, args.parts, parts_dir)
    part_paths = [Path(pw["path"]) for pw in split_info["parts_written"] if pw["lines"] > 0]
    max_disk_bytes = args.max_disk_bytes or None
    max_disk_files = args.max_disk_files or None
    parse_rej = str(ds_dir / "rejections.jsonl")

    def _fetched_path(part: Path) -> Path:
        return part.with_name(part.stem + ".fetched.jsonl")

    def fetch_fn(part: Path) -> bool:
        """Fetch one batch (bulk by default). Returns False if the disk-budget valve stopped
        it part-way, so run_pipeline drains the background parse+purge and resumes THIS part."""
        summary = _fetch(client, str(part), str(_fetched_path(part)), str(raw_dir), args, None)
        return not summary.get("stopped_disk_budget", False)

    def process_fn(part: Path) -> None:
        fetched = str(_fetched_path(part))
        parse(fetched, dataset_dir=str(ds_dir), rejections_path=parse_rej,
              raw_dir=str(raw_dir), max_primary_bytes=args.max_primary_bytes,
              parse_timeout_s=args.parse_timeout)
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
        logging.getLogger(__name__).exception("nomad pipeline fetch failed")
        fetch_error = f"{type(exc).__name__}: {exc}"

    verify = verify_dataset(str(ds_dir))
    summary = {
        "parts": len(part_paths),
        "parts_done": len(done_parts),
        "process_errors": [f"{part}: {type(exc).__name__}: {exc}" for part, exc in errors],
        "fetch_error": fetch_error,
        "verify": verify,
    }
    print(json.dumps(summary, indent=2))
    # Exit non-zero on any integrity/stage failure so a long unattended job's status is honest.
    ok = (not fetch_error and not errors and verify.get("ok", False))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
