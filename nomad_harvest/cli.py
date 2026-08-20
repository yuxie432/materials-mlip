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
        --parts 40 --dataset-dir data/dataset/nomad \
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
import os
import sys
from pathlib import Path

from zenodo_harvest import config
from zenodo_harvest.dataset_ops import purge_raw, verify_dataset
from zenodo_harvest.parse import parse
from zenodo_harvest.store import DatasetLockError

from .client import NomadClient, direct_upload_vasp_query
from .harvest import discover_candidates, fetch_candidates, split_by_upload


def nomad_paths() -> tuple[Path, Path, Path, Path]:
    """NOMAD's OWN ``(root, manifests, raw, dataset)`` — kept SEPARATE from the Zenodo tree
    so the two harvests never share a raw/manifests dir (a concurrent Zenodo job would
    otherwise land in the same staging tree, and the disk valve — which walks its raw dir —
    would count the other harvest's files).

    Root precedence: ``$NOMAD_HARVEST_DATA`` if set, else a **sibling** of the Zenodo data
    root named ``nomad`` when that root is absolute (CSD3 scratch:
    ``/rds/.../hpc-work/zenodo`` → ``/rds/.../hpc-work/nomad``), else nested under it
    (the local ``data`` default → ``data/nomad``, which stays gitignored). The Zenodo dataset
    used for cross-source dedup is unaffected — discover still reads ``config.DATASET_DIR``.
    Call after :func:`config.refresh_paths` so a ``.env`` root is already applied.
    """
    env = os.environ.get("NOMAD_HARVEST_DATA")
    root = Path(env) if env else (
        config.DATA_ROOT.parent / "nomad" if config.DATA_ROOT.is_absolute()
        else config.DATA_ROOT / "nomad")
    return root, root / "manifests", root / "raw", root / "dataset"


def _add_disk_valve(p: argparse.ArgumentParser) -> None:
    """Disk/inode-valve flags (fetch and pipeline both pace with these).

    NB there is no ``--workers`` here: the fetch pulls members out of each upload's pre-packed
    zip via ``GET /uploads/{id}/raw``, which NOMAD rate-limits to **one connection per IP every
    ~5 s**, so it is intrinsically serial (a 2nd connection just 429s). Grouping entries by
    upload — not concurrency — is what keeps the run fast.
    """
    p.add_argument("--max-disk-bytes", type=int, default=0,
                   help="stop staging cleanly once raw/ reaches this many bytes (0 = no "
                        "limit); ~0.8x the CSD3 1 TB hpc-work quota")
    p.add_argument("--max-disk-files", type=int, default=0,
                   help="stop staging once raw/ reaches this many inodes (0 = no limit); "
                        "the CSD3 1M-file quota binds first, so ~800000")
    p.add_argument("--want-outcar", action="store_true",
                   help="also fetch OUTCAR when present (per-atom charges/spins on the final "
                        "frame; larger transfer — vasprun-only is the default)")


def _fetch(client: NomadClient, in_path: str, out_path: str | None, raw_dir: str,
           args: argparse.Namespace, max_records: int | None) -> dict:
    """Run the targeted upload-zip fetch (the only NOMAD fetch path)."""
    return fetch_candidates(client, in_path, raw_dir=raw_dir, out_path=out_path,
                            want_outcar=args.want_outcar, max_records=max_records,
                            max_disk_bytes=(args.max_disk_bytes or None),
                            max_disk_files=(args.max_disk_files or None),
                            # global-skip: don't re-download entries already in the dataset (makes a
                            # PARTS change between runs free of re-fetch). None if the flag is absent.
                            dataset_dir=getattr(args, "dataset_dir", None))


def main(argv: list[str] | None = None) -> int:
    config.load_dotenv()
    config.refresh_paths()  # honour ZENODO_HARVEST_DATA from .env (write to scratch, not /home)
    n_root, n_man, n_raw, n_ds = nomad_paths()  # NOMAD's own tree (separate from Zenodo)
    p = argparse.ArgumentParser(prog="nomad_harvest", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("discover", help="stage 0/1: keyset scan -> license-gated, "
                                        "Zenodo-deduped keep-list")
    d.add_argument("--out", default=str(n_man / "nomad_keep.jsonl"))
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
    f.add_argument("--raw-dir", default=str(n_raw))
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
    pi.add_argument("--raw-dir", default=str(n_raw))
    pi.add_argument("--dataset-dir", default=str(n_ds),
                    help="dataset dir for the NOMAD shards + metadata (kept separate from the "
                         "Zenodo dataset; merge-datasets folds them together later)")
    pi.add_argument("--max-primary-bytes", type=int, default=2_000_000_000,
                    help="RAM guard: refuse to parse a primary bigger than this (0 = off). "
                         "pymatgen peak RSS ~10x file size; size to the job's --mem.")
    pi.add_argument("--parse-timeout", type=int, default=1200,
                    help="hard-kill a single calc's parse after N seconds (0 = off), so one "
                         "non-terminating pymatgen/ASE parse can't freeze the pipeline")
    pi.add_argument("--parse-workers", type=int, default=1,
                    help="parse this many calc units concurrently (default 1 = serial). NOMAD is "
                         "PARSE-throughput-bound (~0.26 s/calc single-threaded -> weeks at 7.1M), "
                         "and its vaspruns are tiny (low RAM), so set this to the node's core "
                         "count to cut the parse ~N-fold. Use with --parse-timeout>0 (true "
                         "process parallelism); size N x per-parse RSS under the job's memory.")
    _add_disk_valve(pi)

    st = sub.add_parser("status", help="read-only snapshot of NOMAD harvest progress "
                                       "(counts, staging vs quota, rejection reasons)")
    st.add_argument("--manifests-dir", default=str(n_man))
    st.add_argument("--raw-dir", default=str(n_raw))
    st.add_argument("--dataset-dir", default=str(n_ds))
    st.add_argument("--keep", default=None,
                    help="keep-list for the fetch %% denominator (default: <manifests>/nomad_keep.jsonl)")
    st.add_argument("--max-disk-bytes", type=int, default=0,
                    help="show staging bytes as %% of this budget (match the pipeline's value)")
    st.add_argument("--max-disk-files", type=int, default=0,
                    help="show staging inodes as %% of this file budget (match the pipeline's value)")
    st.add_argument("--no-staging-walk", action="store_true",
                    help="skip the STAGING walk over raw/ (slow on Lustre with a live job); read "
                         "/rds usage from `lfs quota` instead")
    st.add_argument("--json", action="store_true", help="machine-readable output instead of text")

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
    if args.cmd == "status":
        return _status(args)

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


def _status(args: argparse.Namespace) -> int:
    """Read-only NOMAD progress snapshot. Reuses the shared status walker, told NOMAD's
    manifest names (``nomad_keep.jsonl`` is both the discover output AND the fetch
    denominator — NOMAD folds triage into discover — and its two rejection logs). Safe to
    run while the pipeline writes these files; always exits 0."""
    from zenodo_harvest.status import format_status, status_report
    report = status_report(
        manifests_dir=args.manifests_dir, raw_dir=args.raw_dir, dataset_dir=args.dataset_dir,
        keep_path=args.keep, max_disk_bytes=(args.max_disk_bytes or None),
        max_disk_files=(args.max_disk_files or None), staging_walk=not args.no_staging_walk,
        candidate_globs=["nomad_keep.jsonl"], keep_name="nomad_keep.jsonl",
        extra_rejection_names=("nomad_rejections.jsonl", "nomad_fetch_rejections.jsonl"),
        fetched_globs=["*.fetched.jsonl", "nomad_fetched.jsonl"])
    print(json.dumps(report, indent=2) if args.json else format_status(report))
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
    # Split BY UPLOAD (not round-robin): each upload's entries stay in one part so its zip
    # central directory is read once, not once per part. Same {"parts_written"} shape.
    split_info = split_by_upload(args.in_path, args.parts, parts_dir)
    part_paths = [Path(pw["path"]) for pw in split_info["parts_written"] if pw["lines"] > 0]
    parse_rej = str(ds_dir / "rejections.jsonl")  # disk-valve limits are read from `args` by _fetch

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
              parse_timeout_s=args.parse_timeout, parse_workers=args.parse_workers)
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
