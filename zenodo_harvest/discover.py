"""Stage 0 — discovery.

Turn a set of metadata keyword queries into a deduplicated JSONL manifest of
candidate records. Deduplication is by ``conceptrecid`` (Zenodo's stable
"all versions" id): we keep the newest version of each concept so re-uploads and
minor revisions don't inflate the harvest.

Crash-safe / resumable: an ``--exhaustive`` harvest can run for hours and die
(network hard-fail, OOM, node kill) with nothing on disk if it only wrote the
manifest at the very end. So every accepted raw hit is streamed *immediately* to
an append-only sidecar checkpoint (``<out>.hits.jsonl``, via
:class:`~zenodo_harvest.manifest.JsonlWriter`), interleaved with completion
sentinels — one per leaf ``(query, date-window)`` in exhaustive mode, one per
query in windowed mode. A re-run reloads the sidecar (dedup as usual) and skips
already-completed queries/windows, so no completed API paging is redone. The
final manifest is still written whole, sorted by rank, at the end. ``--fresh``
ignores and removes any existing sidecar to force a clean rebuild.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Iterable

from .client import ZenodoClient
from .manifest import JsonlWriter, read_jsonl
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


def _accept(seen_concept: dict[str, Candidate], cand: Candidate) -> None:
    """Dedup by conceptrecid, keeping the newest version (largest recid is newest)."""
    key = cand.conceptrecid or cand.recid
    prev = seen_concept.get(key)
    if prev is None or int(cand.recid) > int(prev.recid):
        seen_concept[key] = cand


def discover(
    client: ZenodoClient,
    queries: Iterable[str] = DEFAULT_QUERIES,
    out_path: str | Path = "data/manifests/candidates.jsonl",
    resource_types: Iterable[str] = DEFAULT_RESOURCE_TYPES,
    exhaustive: bool = False,
    max_records: int | None = None,
    fresh: bool = False,
) -> dict:
    """Run discovery and write a deduplicated candidate manifest.

    Parameters
    ----------
    exhaustive:
        If True, use recursive date-partitioning to get past the 10k window per
        query (full-harvest mode, for the cluster). If False, take up to the
        first 10k per query (fast trials on WSL).
    max_records:
        Hard cap on total unique concepts written (handy for trials). Counts
        resumed concepts too (same semantics whether fresh or resuming).
    fresh:
        Ignore and delete any existing ``<out>.hits.jsonl`` checkpoint, forcing a
        clean rebuild rather than resuming from it.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    queries = list(queries)  # materialise: consumed once, reused in the summary
    sidecar_path = Path(str(out_path) + ".hits.jsonl")

    extra: dict = {}
    rts = list(resource_types)
    if len(rts) == 1:
        extra["type"] = rts[0]  # single-value filter Zenodo supports directly

    seen_concept: dict[str, Candidate] = {}
    done_windows: set[tuple[str, str, str]] = set()  # (query, start.iso, end.iso)
    done_queries: set[str] = set()

    # -- resume: replay a prior sidecar checkpoint, if any -------------------
    resumed_hits = 0
    if fresh:
        sidecar_path.unlink(missing_ok=True)
    elif sidecar_path.is_file():
        for row in read_jsonl(sidecar_path):
            kind = row.get("kind")
            if kind == "hit":
                _accept(seen_concept, Candidate(**row["candidate"]))
                resumed_hits += 1
            elif kind == "window":
                done_windows.add((row["query"], row["start"], row["end"]))
            elif kind == "query":
                done_queries.add(row["query"])
    resumed_concepts = len(seen_concept)

    scanned = 0
    counters = {"skipped_windows": 0, "skipped_queries": 0, "skipped_access_right": 0}
    reached_cap = bool(max_records and len(seen_concept) >= max_records)

    def _keep(rec: dict, q: str) -> None:
        nonlocal scanned
        scanned += 1
        rec.setdefault("_query", q)
        # 0 types => no filter; 1 => server-side (extra["type"]); >1 => filter here.
        if len(rts) > 1:
            rt = rec.get("metadata", {}).get("resource_type") or {}
            rt = rt.get("type") if isinstance(rt, dict) else rt
            if rt not in rts:
                return
        cand = Candidate.from_record(rec)
        # access_right gate: non-open records 403 at fetch anyway, so drop them here
        # to save doomed downloads and keep the rejection log clean. A missing value
        # is treated as open (kept). NB: license stays TAG-ONLY (never a gate) — a
        # license-allowlist gate is still an open decision with the mentor.
        if cand.access_right is not None and cand.access_right != "open":
            counters["skipped_access_right"] += 1
            return
        sidecar.write({"kind": "hit", "candidate": cand.to_dict()})  # persist ASAP
        _accept(seen_concept, cand)

    with JsonlWriter(sidecar_path) as sidecar:
        for q in queries:
            if reached_cap:
                break
            if not exhaustive and q in done_queries:
                counters["skipped_queries"] += 1
                continue

            if exhaustive:
                def should_skip(s: date, e: date, _q: str = q) -> bool:
                    if (_q, s.isoformat(), e.isoformat()) in done_windows:
                        counters["skipped_windows"] += 1
                        return True
                    return False

                def on_window_done(s: date, e: date, _q: str = q) -> None:
                    key = (_q, s.isoformat(), e.isoformat())
                    sidecar.write({"kind": "window", "query": _q,
                                   "start": s.isoformat(), "end": e.isoformat()})
                    done_windows.add(key)

                stream = client.iter_records(q, extra=extra, should_skip=should_skip,
                                             on_window_done=on_window_done)
            else:
                stream = client.iter_window(q, extra=extra)

            for rec in stream:
                _keep(rec, q)
                if max_records and len(seen_concept) >= max_records:
                    reached_cap = True
                    break

            # A query only completes if we consumed it fully (not cut off by the cap).
            if not exhaustive and not reached_cap:
                sidecar.write({"kind": "query", "query": q})
                done_queries.add(q)

    cat_counter: Counter = Counter()
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
        "sidecar_path": str(sidecar_path),
        "resumed_hits": resumed_hits,
        "resumed_concepts": resumed_concepts,
        "skipped_windows": counters["skipped_windows"],
        "skipped_queries": counters["skipped_queries"],
        "skipped_access_right": counters["skipped_access_right"],
    }
    logger.info("discovery summary: %s", summary)
    return summary
