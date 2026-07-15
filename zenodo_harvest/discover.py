"""Stage 0 — discovery.

Turn a set of metadata keyword queries into a deduplicated JSONL manifest of
candidate records. Deduplication is by ``conceptrecid`` (Zenodo's stable
"all versions" id): we keep the newest version of each concept so re-uploads and
minor revisions don't inflate the harvest.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Iterable

from .client import ZenodoClient, iter_many
from .models import Candidate

logger = logging.getLogger(__name__)

# Broad, recall-oriented seed queries. Zenodo's default operator is OR, so keep
# multi-word phrases quoted when you mean a phrase. These target materials DFT;
# tune per harvest. Precision is recovered later in triage + parse. NB: ``q`` is
# metadata-only (title/description/keywords), never file contents — so we also cast
# for method/tooling terms and the code's own filenames, which uploaders often name.
DEFAULT_QUERIES = [
    "VASP",
    "vasprun.xml",
    "OUTCAR",
    "INCAR",
    '"projector augmented wave" OR "plane wave" OR PAW',
    '"first principles" AND (energy OR forces OR relaxation OR "molecular dynamics")',
    '"density functional theory" AND (dataset OR trajectory OR forces OR relaxation)',
    '"machine learning" AND (interatomic potential OR force field) AND DFT',
    "MACE OR NequIP OR Allegro OR GAP AND training data",
    '"ab initio" molecular dynamics forces',
    'phonon AND (DFT OR VASP OR "first principles")',
    '"formation energy" AND (DFT OR VASP OR "first principles")',
]

# Resource types worth scanning by default — a *quality-leaning* prior, not maximal
# recall. `dataset` is curated research data (the core); `other` is cheap (~few
# records) and occasionally exposes a raw OUTCAR/POSCAR dump directly. Deliberately
# EXCLUDED from the default: `software` (VASP data there is usually small
# tutorial/example runs) and `publication` (mostly PDFs; real data is normally
# re-deposited as its own dataset) — both add download/parse cost for lower-quality
# yield. They remain one `--resource-type` flag away when you want to widen. Whatever
# is scanned, the `resource_type` is recorded in the metadata so a training set can
# be filtered/weighted by source (alongside the convergence / ENCUT / k-point tags).
DEFAULT_RESOURCE_TYPES = ("dataset", "other")


def discover(
    client: ZenodoClient,
    queries: Iterable[str] = DEFAULT_QUERIES,
    out_path: str | Path = "data/manifests/candidates.jsonl",
    resource_types: Iterable[str] = DEFAULT_RESOURCE_TYPES,
    exhaustive: bool = False,
    max_records: int | None = None,
) -> dict:
    """Run discovery and write a deduplicated candidate manifest.

    Parameters
    ----------
    exhaustive:
        If True, use recursive date-partitioning to get past the 10k window per
        query (full-harvest mode, for the cluster). If False, take up to the
        first 10k per query (fast trials on WSL).
    max_records:
        Hard cap on total records written (handy for trials).
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    queries = list(queries)  # materialise: iter_many consumes it, the summary reuses it

    extra: dict = {}
    rts = list(resource_types)
    if len(rts) == 1:
        extra["type"] = rts[0]  # single-value filter Zenodo supports directly

    seen_concept: dict[str, Candidate] = {}
    cat_counter: Counter = Counter()
    scanned = 0

    for rec in iter_many(client, queries, extra=extra, exhaustive=exhaustive):
        scanned += 1
        # 0 types => no filter; 1 => server-side (extra["type"]); >1 => filter here.
        if len(rts) > 1:
            rt = (rec.get("metadata", {}).get("resource_type") or {})
            rt = rt.get("type") if isinstance(rt, dict) else rt
            if rt not in rts:
                continue
        cand = Candidate.from_record(rec)
        key = cand.conceptrecid or cand.recid
        prev = seen_concept.get(key)
        # Keep the newest version (largest recid is newest on Zenodo).
        if prev is None or int(cand.recid) > int(prev.recid):
            seen_concept[key] = cand
        if max_records and len(seen_concept) >= max_records:
            break

    with out_path.open("w") as fh:
        for cand in sorted(seen_concept.values(), key=lambda c: c.vasp_rank, reverse=True):
            cat_counter[cand.vasp_category] += 1
            fh.write(json.dumps(cand.to_dict()) + "\n")

    summary = {
        "queries": queries,
        "records_scanned": scanned,
        "unique_concepts": len(seen_concept),
        "by_category": dict(cat_counter),
        "out_path": str(out_path),
    }
    logger.info("discovery summary: %s", summary)
    return summary
