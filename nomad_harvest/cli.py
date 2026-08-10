"""Command-line entrypoints for the NOMAD harvest (stages 0-2).

Stages 3-5 are shared with Zenodo — after ``fetch`` writes ``nomad_fetched.jsonl``, parse
and verify it with the existing pipeline::

    python -m nomad_harvest.cli smoke -n 12                 # Phase-0 validation (isolated)
    python -m nomad_harvest.cli discover --elements Ti O \
        --out data/manifests/nomad_keep.jsonl              # keyset scan + gate + dedup
    python -m nomad_harvest.cli fetch --in data/manifests/nomad_keep.jsonl \
        --out data/manifests/nomad_fetched.jsonl           # stage vasprun files
    python -m zenodo_harvest.cli parse  --in data/manifests/nomad_fetched.jsonl \
        --dataset-dir data/dataset/nomad                   # SHARED parser
    python -m zenodo_harvest.cli verify --dataset-dir data/dataset/nomad
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from zenodo_harvest import config

from .client import NomadClient, direct_upload_vasp_query
from .harvest import discover_candidates, fetch_candidates


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
    d.add_argument("--max-entries", type=int, default=None)
    d.add_argument("--page-size", type=int, default=1000)
    d.add_argument("--zenodo-metadata", default=str(config.DATASET_DIR / "metadata.jsonl"),
                   help="Zenodo dataset metadata.jsonl to dedup against (skipped if absent)")
    d.add_argument("--no-license-gate", dest="license_gate", action="store_false")

    f = sub.add_parser("fetch", help="stage 2: stage each candidate's vasprun -> fetched.jsonl")
    f.add_argument("--in", dest="in_path", required=True, help="keep-list JSONL from discover")
    f.add_argument("--out", default=None,
                   help="fetched manifest (default: <raw-dir>/../manifests/nomad_fetched.jsonl)")
    f.add_argument("--raw-dir", default=str(config.RAW_DIR))
    f.add_argument("--want-outcar", action="store_true",
                   help="also fetch OUTCAR when present (per-atom charges/spins; larger)")
    f.add_argument("--max-records", type=int, default=None)

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
        work = Path(args.work_dir) if args.work_dir else None
        import tempfile
        work = work or Path(tempfile.mkdtemp(prefix="nomad_smoke_"))
        return run(args.n, args.elements, work, args.keep)

    client = NomadClient()
    if args.cmd == "discover":
        zmeta = args.zenodo_metadata if Path(args.zenodo_metadata).is_file() else None
        summary = discover_candidates(
            client, args.out, query=direct_upload_vasp_query(elements=args.elements),
            max_entries=args.max_entries, page_size=args.page_size,
            license_gate=args.license_gate, zenodo_metadata=zmeta)
    elif args.cmd == "fetch":
        summary = fetch_candidates(client, args.in_path, raw_dir=args.raw_dir,
                                   out_path=args.out, want_outcar=args.want_outcar,
                                   max_records=args.max_records)
    else:  # pragma: no cover
        p.error(f"unknown command {args.cmd}")

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
