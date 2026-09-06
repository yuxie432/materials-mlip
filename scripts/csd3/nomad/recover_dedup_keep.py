#!/usr/bin/env python3
"""Rebuild a keep-list for NOMAD entries WRONGLY dropped as ``duplicate_of_zenodo``.

Root cause (investigated 2026-09-06): NOMAD discover's dedup (``harvest.zenodo_overlap``)
drops any entry whose ``references``/``datasets[].doi`` contain a ``10.5281/zenodo.*`` DOI —
via the broad ``d.startswith("10.5281/zenodo.")`` branch — even when that Zenodo record is
only *cited* (not the data) and was never harvested. In this harvest that discarded ~18,615
unique VASP-DFT calcs that merely cite one Zenodo *code* release (10.5281/zenodo.18598420),
which holds no VASP data and is absent from the Zenodo dataset.

This rebuilds a keep-list of exactly those false positives: entries dropped as duplicates
whose referenced Zenodo DOI is NOT in the harvested Zenodo set. It re-queries their full
metadata from NOMAD (the ``entries/query`` bucket — light, not the throttled uploads one)
and applies the NARROWED dedup (drop only DOIs we truly hold), so a genuine Zenodo dup is
still excluded. Feed the output to ``nomad_harvest.cli pipeline`` — the global dataset-skip
means only these new entries are fetched+parsed into the existing dataset.

Resumable: entries already in the output keep-list are skipped. Idempotent. Needs outbound
HTTPS, so run it on a COMPUTE node (the wrapper recover_dedup.sh does).

    python scripts/csd3/nomad/recover_dedup_keep.py        # writes $NOMAD/manifests/nomad_recover_keep.jsonl
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Put the repo root on sys.path so `python scripts/csd3/nomad/recover_dedup_keep.py` (which
# sets sys.path[0] to the SCRIPT's dir, not the repo root) can import the harvest packages —
# the same reason the harvest itself is always launched as `python -m nomad_harvest.cli`.
for _root in Path(__file__).resolve().parents:
    if (_root / "nomad_harvest").is_dir():
        sys.path.insert(0, str(_root))
        break

from nomad_harvest import harvest
from nomad_harvest.client import CANDIDATE_REQUIRED, NomadClient
from zenodo_harvest.manifest import JsonlWriter, read_jsonl
from zenodo_harvest.models import is_reusable_license

USER = os.environ["USER"]
NOMAD = Path(os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/nomad"))
ZENODO = Path(os.environ.get("ZENODO_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/zenodo"))
MAN = NOMAD / "manifests"
ZEN_META = ZENODO / "dataset" / "metadata.jsonl"
REJ = MAN / "nomad_rejections.jsonl"
OUT = MAN / "nomad_recover_keep.jsonl"
BATCH = 500  # entry_ids per entries/query (well within the API's POST-body limit)


def main() -> int:
    if not REJ.is_file():
        print(f"ERROR: {REJ} not found", file=sys.stderr)
        return 2
    # DOIs actually harvested from Zenodo — the ONLY DOIs a real duplicate can match.
    known = harvest.zenodo_dois(ZEN_META)
    print(f"[recover] harvested Zenodo DOIs: {len(known)}  (from {ZEN_META})", file=sys.stderr)

    # Candidate false positives: entries dropped as duplicate_of_zenodo whose detail DOI is
    # NOT one we hold. (The detail records only the FIRST matching ref; the authoritative
    # narrowed re-check over ALL refs happens after re-query below.)
    cand: set[str] = set()
    for line in REJ.open():
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        if r.get("reason") != "duplicate_of_zenodo":
            continue
        detail = str(r.get("detail", ""))
        doi = detail[len("shared_doi:"):].lower() if detail.startswith("shared_doi:") else None
        if doi and doi not in known and r.get("id"):
            cand.add(str(r["id"]))
    print(f"[recover] candidate false-positive entries: {len(cand)}", file=sys.stderr)

    done = {rec["entry_id"] for rec in read_jsonl(OUT)} if OUT.is_file() else set()
    todo = sorted(cand - done)
    print(f"[recover] already built: {len(done)}   to query now: {len(todo)}", file=sys.stderr)
    if not todo:
        print(f"[recover] nothing to do -> {OUT}", file=sys.stderr)
        return 0

    client = NomadClient()  # anonymous reads
    kept = requeried = 0
    with JsonlWriter(OUT) as w:
        for i in range(0, len(todo), BATCH):
            batch = todo[i:i + BATCH]
            for e in client.iter_entries({"entry_id:any": batch},
                                         required=CANDIDATE_REQUIRED, page_size=BATCH):
                requeried += 1
                if not is_reusable_license(e.get("license")):
                    continue
                # NARROWED dedup: drop only if a referenced DOI is a record we truly harvested.
                dois = [harvest.normalize_doi(r) for r in harvest.references_of(e)]
                if any(d in known for d in dois if d):
                    continue
                w.write(harvest.slim_candidate(e))
                kept += 1
            print(f"[recover] queried {min(i + BATCH, len(todo))}/{len(todo)}"
                  f"  (re-queried {requeried}, kept {kept})", file=sys.stderr)

    print(f"[recover] DONE: kept {kept} entries -> {OUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
