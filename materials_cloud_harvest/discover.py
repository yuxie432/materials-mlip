"""Stage 0 — discover: a FULL census of the Materials Cloud Archive → candidate manifest.

Unlike Zenodo (7.3M records behind a 30 req/min, metadata-text-only search), the whole MC
Archive is ~1.2k records, so discovery enumerates EVERY record (~13 paged requests) instead of
relying on keyword recall — which removes the metadata blind spot at the listing level (a record
that never says "VASP" still reaches triage, where its archives are peeked). Keywords survive only
as a *signal* (``vasp_mention``) that decides how fail-safe triage is for that record.

Gates (each drop logged with a machine reason to ``mc_rejections.jsonl`` — recall stays auditable):

* **access** — non-public records/files or an active embargo (none observed; guarded);
* **licence** — :func:`records.licence_verdict` under the chosen policy (default ``nc-ok``);
* **manual exclusion** — ``exclude_ids``.

Overlap with the already-harvested sources is **flagged, never auto-dropped** (unless
``drop_linked``): every observed MC→Zenodo link is ``IsSupplementTo`` — e.g. Kavanagh's MC record
holds the heavy raw VASP that SUPPLEMENTS his Zenodo deposit — so a record-level drop would lose
real data (the NOMAD harvest learnt the same lesson: citing ≠ duplicating). The flags
(``overlap`` on the candidate, ``linked_harvested`` in provenance) feed the planned training-time
physics-level dedup instead.
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

from zenodo_harvest.manifest import RejectionLogger, read_jsonl

from .client import MaterialsCloudClient
from .records import (
    licence_verdict,
    linked_dois,
    mcid_base,
    normalize_doi,
    record_to_candidate,
    title_similarity,
    title_tokens,
)

logger = logging.getLogger(__name__)

# Title-similarity (token Jaccard) at/above which an MC record is flagged as a possible re-deposit
# of a record already in the Zenodo dataset (a FLAG for review, never a drop).
TITLE_SIMILARITY_FLAG = 0.6


def zenodo_index(metadata_path: str | Path) -> dict[str, Any]:
    """DOIs + titles of the records in the harvested Zenodo dataset (for overlap FLAGS).

    Streams ``metadata.jsonl`` once (one line per calc; each record repeats its provenance), so it
    holds only one small entry per distinct record. Missing file → empty index (no flags)."""
    dois: dict[str, str] = {}          # doi/conceptdoi (normalised) -> zenodo record_id
    titles: dict[str, frozenset[str]] = {}
    p = Path(metadata_path)
    if not p.is_file():
        return {"dois": dois, "titles": titles}
    for rec in read_jsonl(p):
        prov = rec.get("provenance") or {}
        rid = str(prov.get("record_id") or "")
        if not rid or rid in titles:
            continue
        for key in ("doi", "conceptdoi"):
            d = normalize_doi(prov.get(key))
            if d:
                dois[d] = rid
        titles[rid] = title_tokens(prov.get("title"))
    return {"dois": dois, "titles": titles}


def _mc_keys(ref: str) -> set[str]:
    """Canonical keys an MC reference string resolves to: ``mcid:<2020.0006>`` (version DOIs and
    old-style archive URLs carry the short id) and/or ``doi:<normalised MC DOI>`` (concept DOIs
    such as ``10.24435/materialscloud:wm-6j`` carry no short id)."""
    keys: set[str] = set()
    d = normalize_doi(ref)
    if d and "materialscloud" in d:
        keys.add(f"doi:{d}")
    m = mcid_base(ref)
    if m:
        keys.add(f"mcid:{m}")
    return keys


def candidate_mc_keys(cand: dict[str, Any]) -> set[str]:
    """The canonical keys a NOMAD reference to THIS MC record could resolve to."""
    keys: set[str] = set()
    for field in ("doi", "conceptdoi", "mcid"):
        if cand.get(field):
            keys |= _mc_keys(str(cand[field]))
    return keys


def nomad_mc_references(metadata_path: str | Path) -> dict[str, set[int]]:
    """Which NOMAD calcs (by line number) cite each Materials Cloud record key (overlap FLAGS).

    NOMAD's ``metadata.jsonl`` is ~7M lines, so each line is first tested for the substring
    ``materialscloud`` (cheap) and only those are parsed. Keys come from :func:`_mc_keys`; a calc
    citing one record by several identifiers (version DOI + concept DOI + URL) is still ONE calc,
    so :func:`overlap_flags` counts the union of the line sets of a record's keys."""
    out: dict[str, set[int]] = defaultdict(set)
    p = Path(metadata_path)
    if not p.is_file():
        return out
    with p.open() as fh:
        for n, line in enumerate(fh):
            if "materialscloud" not in line.lower():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            prov = rec.get("provenance") or {}
            refs = [str(r) for r in (prov.get("references") or [])]
            if prov.get("doi"):
                refs.append(str(prov["doi"]))
            for ref in refs:
                if "materialscloud" in ref.lower():
                    for k in _mc_keys(ref):
                        out[k].add(n)
    return out


def overlap_flags(cand: dict[str, Any], zindex: dict[str, Any] | None,
                  nomad_refs: dict[str, set[int]] | None) -> dict[str, Any]:
    """The overlap evidence for one candidate (empty dict = no overlap signal)."""
    flags: dict[str, Any] = {}
    if zindex:
        zdois = zindex.get("dois") or {}
        linked = sorted({f"{d} (zenodo:{zdois[d]})" for d in linked_dois(cand) if d in zdois})
        if linked:
            flags["zenodo_linked_in_dataset"] = linked
        mine = title_tokens(cand.get("title"))
        similar = []
        for rid, toks in (zindex.get("titles") or {}).items():
            s = title_similarity(mine, toks)
            if s >= TITLE_SIMILARITY_FLAG:
                similar.append({"zenodo_record_id": rid, "similarity": round(s, 3)})
        if similar:
            flags["zenodo_title_similar"] = sorted(similar, key=lambda x: -x["similarity"])[:5]
    if nomad_refs:
        citing: set[int] = set()
        for k in candidate_mc_keys(cand):
            citing |= nomad_refs.get(k, set())
        if citing:
            flags["nomad_calcs_citing"] = len(citing)
    return flags


def discover(client: MaterialsCloudClient, out_path: str | Path, *,
             rejections_path: str | Path | None = None,
             licence_policy: str = "nc-ok",
             exclude_ids: Iterable[str] = (),
             only_ids: Iterable[str] | None = None,
             zenodo_metadata: str | Path | None = None,
             nomad_metadata: str | Path | None = None,
             drop_linked: bool = False,
             max_records: int | None = None,
             records: Iterable[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Enumerate the archive, gate, flag overlap, and write the candidate manifest.

    ``only_ids`` restricts to the given record ids (fetched one by one — smoke tests, by-id
    additions); otherwise every record is enumerated. ``records`` injects already-fetched API
    records (tests / an offline census). Rewrites ``out_path`` (the census is cheap to redo);
    candidates are ordered VASP-mentioning first, then by rank, then id (deterministic).
    """
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    rej_path = Path(rejections_path) if rejections_path else out.parent / "mc_rejections.jsonl"
    exclude = {str(x) for x in exclude_ids}
    zindex = zenodo_index(zenodo_metadata) if zenodo_metadata else None
    nrefs = nomad_mc_references(nomad_metadata) if nomad_metadata else None

    if records is not None:
        stream: Iterable[dict[str, Any]] = records
    elif only_ids is not None:
        stream = (client.get_record(str(i)) for i in only_ids)
    else:
        stream = client.iter_records(max_records=max_records)

    stats: Counter = Counter()
    dropped: dict[str, list[str]] = defaultdict(list)
    kept: list[dict[str, Any]] = []
    seen_concepts: dict[str, int] = {}
    with RejectionLogger(rej_path) as rej:
        for rec in stream:
            stats["scanned"] += 1
            cand = record_to_candidate(rec, base=client.base)
            rid = cand["recid"]
            if cand.get("files_count_declared") not in (None, cand["files_total"]):
                # never observed (the search hit lists every file) — refill from /files if so
                try:
                    rec = {**rec, "files": {"entries": client.list_files(rid)}}
                    cand = record_to_candidate(rec, base=client.base)
                    stats["files_refetched"] += 1
                except Exception as exc:  # noqa: BLE001 - keep the partial listing, log it
                    logger.warning("file listing refill failed for %s: %s", rid, exc)
            reason = None
            if rid in exclude:
                reason = "manually_excluded"
            elif cand["access_right"] != "open":
                reason = "not_open_access"
            else:
                ok, why = licence_verdict(cand.get("license"), licence_policy)
                if not ok:
                    reason = f"non_redistributable_license:{why}"
            flags = overlap_flags(cand, zindex, nrefs) if reason is None else {}
            if reason is None and drop_linked and flags.get("zenodo_linked_in_dataset"):
                reason = "linked_to_harvested_zenodo"
            if reason is not None:
                rej.reject("mc_discover", rid, reason, license=cand.get("license"),
                           title=cand.get("title", "")[:120], vasp_mention=cand["vasp_mention"])
                stats["dropped"] += 1
                dropped[reason.split(":")[0]].append(rid)
                continue
            if flags:
                cand["overlap"] = flags
                cand["provenance"]["linked_harvested"] = flags
                stats["flagged_overlap"] += 1
            concept = str(cand.get("conceptrecid") or rid)
            if concept in seen_concepts:          # the search returns latest versions only;
                prev = kept[seen_concepts[concept]]  # keep the newest if a duplicate slips in
                if (cand.get("version_index") or 0) <= (prev.get("version_index") or 0):
                    stats["duplicate_concept"] += 1
                    continue
                kept[seen_concepts[concept]] = cand
                stats["duplicate_concept"] += 1
                continue
            seen_concepts[concept] = len(kept)
            kept.append(cand)

    kept.sort(key=lambda c: (not c["vasp_mention"], -int(c["vasp_rank"]), c["recid"]))
    with out.open("w") as fh:
        for c in kept:
            fh.write(json.dumps(c) + "\n")
    by_cat = Counter(c["vasp_category"] for c in kept)
    summary = {
        "out": str(out), "rejections": str(rej_path),
        "scanned": stats["scanned"], "candidates": len(kept), "dropped": stats["dropped"],
        "dropped_by_reason": {k: len(v) for k, v in dropped.items()},
        "vasp_mention": sum(1 for c in kept if c["vasp_mention"]),
        "by_category": dict(by_cat),
        "bytes_total": sum(c["bytes_total"] for c in kept),
        "flagged_overlap": stats["flagged_overlap"],
        "overlap_examples": [{"recid": c["recid"], "title": c["title"][:80], **c["overlap"]}
                             for c in kept if c.get("overlap")][:25],
        "licence_policy": licence_policy,
        "zenodo_index_records": len((zindex or {}).get("titles") or {}),
        "nomad_calcs_citing_mc": len(set().union(*nrefs.values())) if nrefs else None,
    }
    logger.info("mc discover: %s", {k: v for k, v in summary.items() if k != "overlap_examples"})
    return summary
