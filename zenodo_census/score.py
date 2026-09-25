"""Stage 0'' — score every census record into a tier (offline; the link channels ran first).

Inputs: the census, the Zenodo VASP dataset's ``metadata.jsonl`` (its records are the seeds and are
excluded), the keyword harvest's candidate / keep manifests (records the keyword method already
evaluated are excluded — their verdicts stand), optional extra seed creators (e.g. the Materials
Cloud dataset's), and the three link tables of :mod:`zenodo_census.links`.

Output: ``scored.jsonl`` — one line per census record: tier, reasons, the compact signals, licence
class, and a summary of its archives (what triage will have to peek) — plus ``score_report.json``
(tier x resource-type counts, the peek workload, the known-miss probes).
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Iterator

from zenodo_harvest.fetch import _is_archive
from zenodo_harvest.manifest import read_jsonl
from zenodo_harvest.models import ARCHIVE_EXTS as OLD_TRIAGE_ARCHIVE_EXTS

from .census import iter_census
from .signals import (
    ADMITTED_LICENCES,
    VASP_DOIS,
    Seeds,
    identity_signals,
    licence_class,
    licence_id,
    norm_name,
    paper_dois,
    text_signals,
    tier_of,
)

logger = logging.getLogger(__name__)

MAX_CITING = 5          # citing papers looked up per record (datasets rarely have more)
# The four Kavanagh VASP datasets the keyword harvest missed (3 by discovery, 1 by the no-licence
# gate). A recall probe only — they get no special treatment.
PROBE_RECIDS = ("13888307", "4541602", "12518256", "10630244")
_ZIP_KINDS = {"zip"}


# A keyword candidate the old triage dropped with a zip at least this big is re-checked: the
# 16-bit entry-count bug (fixed 2026-09-24) could make its peek stop early and "prove" a zip with
# >65,535 members VASP-free, and such a zip (central directory alone >= ~3 MB) is rarely small.
RECHECK_ZIP_BYTES = 100_000_000


def _census_archive_kind(key: str) -> str | None:
    base = key.rsplit("/", 1)[-1]
    return "aiida" if base.lower().endswith(".aiida") else _is_archive(base)


def needs_recheck(row: dict[str, Any]) -> str | None:
    """Why a keyword-harvest candidate the old triage DROPPED deserves another look, or None.

    * its archives were invisible to the old classifier — ``.aiida``, ``.txz``, ``.tzst``, bare
      ``.zst`` … are archives to the fetch but not to ``models.ARCHIVE_EXTS``, so the record ranked
      below the triage gate and was never peeked or fetched;
    * it holds a zip of >= ``RECHECK_ZIP_BYTES`` that the old peek (16-bit count bug) may have
      "proven" empty."""
    files = row.get("files") or []
    for f in files:
        key = str(f.get("key") or "")
        base = key.rsplit("/", 1)[-1].lower()
        ext = "." + base.rsplit(".", 1)[-1] if "." in base else ""
        for dbl in (".tar.gz", ".tar.bz2", ".tar.xz"):
            if base.endswith(dbl):
                ext = dbl
        if _census_archive_kind(key) and ext not in OLD_TRIAGE_ARCHIVE_EXTS:
            return f"archive invisible to the old triage: {key}"
    if int(row.get("vasp_rank") or 0) >= 3:
        for f in files:
            if (_census_archive_kind(str(f.get("key") or "")) == "zip"
                    and int(f.get("size") or 0) >= RECHECK_ZIP_BYTES):
                return f"large zip under the old 16-bit-count peek: {f.get('key')}"
    return None


class Exclusions:
    """Records the census must not re-harvest: those in the dataset, and those the keyword
    harvest already evaluated. Keep-lists (fetched records) are always final; a candidate the old
    triage DROPPED is final unless :func:`needs_recheck` says the old code could not really examine
    it. Matched on recid OR conceptrecid, so a newer version of an evaluated record is out too."""

    def __init__(self) -> None:
        self.dataset_recids: set[str] = set()
        self.dataset_concepts: set[str] = set()
        self.evaluated_recids: set[str] = set()
        self.evaluated_concepts: set[str] = set()
        self.dataset_names: set[str] = set()
        self._kept: set[str] = set()
        self._dropped: dict[str, tuple[str | None, str]] = {}   # recid -> (concept, recheck why)
        self.recheck: dict[str, str] = {}
        self.stats: Counter = Counter()

    def add_dataset(self, metadata_path: str | Path) -> None:
        """The dataset's own records (+ their creator names, for records whose latest version
        is not in the census)."""
        seen: set[str] = set()
        for row in read_jsonl(metadata_path):
            prov = row.get("provenance") or {}
            if prov.get("source", "zenodo") != "zenodo":
                continue
            rid = str(prov.get("record_id") or "")
            if not rid or rid in seen:
                continue
            seen.add(rid)
            self.dataset_recids.add(rid)
            if prov.get("conceptrecid"):
                self.dataset_concepts.add(str(prov["conceptrecid"]))
            for c in prov.get("creators") or []:
                nn = norm_name(str(c))
                if nn:
                    self.dataset_names.add(nn)
        self.stats["dataset_records"] = len(self.dataset_recids)

    def add_manifest(self, path: str | Path, keep: bool | None = None) -> int:
        """Add a keyword-harvest manifest: a keep-list (``keep`` True; by default any file whose
        name contains "keep") or a candidate manifest."""
        is_keep = ("keep" in Path(path).name) if keep is None else keep
        n = 0
        for row in read_jsonl(path):
            rid = str(row.get("recid") or "").split("~", 1)[0]
            if not rid:
                continue
            n += 1
            concept = str(row["conceptrecid"]) if row.get("conceptrecid") else None
            self.evaluated_recids.add(rid)
            if concept:
                self.evaluated_concepts.add(concept)
            if is_keep:
                self._kept.add(rid)
                if concept:
                    self._kept.add(concept)
            else:
                why = needs_recheck(row)
                if why:
                    self._dropped[rid] = (concept, why)
        self.stats[f"manifest:{Path(path).name}"] = n
        self._resolve()
        return n

    def _resolve(self) -> None:
        """Re-include dropped-but-unexamined candidates (never a kept one)."""
        for rid, (concept, why) in self._dropped.items():
            if rid in self._kept or (concept and concept in self._kept):
                self.recheck.pop(rid, None)
                continue
            self.recheck[rid] = why
        self.stats["recheck"] = len(self.recheck)

    def status(self, rec: dict[str, Any]) -> str | None:
        rid = str(rec.get("id"))
        concept = str(rec.get("conceptrecid") or rid)
        if rid in self.dataset_recids or concept in self.dataset_concepts:
            return "in_dataset"
        if rid in self.recheck:
            return None
        if rid in self.evaluated_recids or concept in self.evaluated_concepts:
            return "evaluated"
        return None


def build_seeds(census_path: str | Path, excl: Exclusions,
                extra_seed_metadata: Iterable[str | Path] = ()) -> Seeds:
    """One census pass: census-wide frequencies + the identities of in-dataset records; then the
    dataset's own creator names (covers records whose latest version holds no archive) and the
    creators of any extra seed dataset (e.g. Materials Cloud's ``metadata.jsonl``)."""
    seeds = Seeds()
    n_seed = 0
    for rec in iter_census(census_path):
        seeds.count(rec)
        if excl.status(rec) == "in_dataset":
            seeds.add_record(rec)
            n_seed += 1
    seeds.names.update(excl.dataset_names)
    for path in extra_seed_metadata:
        seen: set[str] = set()
        for row in read_jsonl(path):
            prov = row.get("provenance") or {}
            key = str(prov.get("record_id") or "")
            if key in seen:
                continue
            seen.add(key)
            for c in prov.get("creators") or []:
                nn = norm_name(str(c))
                if nn:
                    seeds.names.add(nn)
    logger.info("seeds: %d in-dataset census records; %d owners, %d ORCIDs, %d names, "
                "%d communities", n_seed, len(seeds.owners), len(seeds.orcids),
                len(seeds.names), len(seeds.communities))
    return seeds


def archive_summary(files: list[dict[str, Any]]) -> dict[str, Any]:
    """What triage must do for a record: zip-peekable vs other archives, and their bytes."""
    s = {"n_zip": 0, "n_unpeekable": 0, "n_aiida": 0, "bytes_zip": 0, "bytes_unpeekable": 0}
    for f in files:
        key = str(f.get("key") or "")
        size = int(f.get("size") or 0)
        base = key.rsplit("/", 1)[-1]
        if base.lower().endswith(".aiida"):
            s["n_aiida"] += 1
            s["bytes_zip"] += size
            continue
        kind = _is_archive(base)
        if kind in _ZIP_KINDS:
            s["n_zip"] += 1
            s["bytes_zip"] += size
        elif kind is not None:
            s["n_unpeekable"] += 1
            s["bytes_unpeekable"] += size
    return s


def _zkeys(rec: dict[str, Any]) -> set[str]:
    return {str(rec.get("id")), str(rec.get("conceptrecid") or rec.get("id"))}


def record_signals(rec: dict[str, Any], seeds: Seeds,
                   citing: dict[str, set[str]] | None = None,
                   epmc: dict[str, set[str]] | None = None,
                   openalex: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """All signals for one census record (papers included when the link tables are given)."""
    meta = rec.get("metadata") or {}
    sig = {**text_signals(rec), **identity_signals(rec, seeds)}
    dois = paper_dois(meta)
    cites: set[str] = set()
    ep: set[str] = set()
    for k in _zkeys(rec):
        cites |= (citing or {}).get(k, set())
        ep |= (epmc or {}).get(k, set())
    cites_l = sorted(cites)[:MAX_CITING]
    sig["paper_dois"] = dois[:10]
    sig["vasp_paper_linked"] = [d for d in dois if d in VASP_DOIS]
    sig["citing_dois"] = cites_l
    sig["epmc"] = sorted(ep)
    oa = [(openalex or {}).get(d) for d in (*dois, *cites_l)]
    ok = [r for r in oa if r and r.get("status") == 200]
    sig["paper_vasp"] = sorted({r["doi"] for r in ok if r.get("cites_vasp")})
    sig["paper_fields"] = sorted({str(r["field"]) for r in ok if r.get("field")})
    return sig


def lookup_dois(census_path: str | Path, seeds: Seeds, excl: Exclusions,
                citing: dict[str, set[str]], include_t1: bool = False,
                epmc: dict[str, set[str]] | None = None) -> set[str]:
    """The paper DOIs worth an OpenAlex lookup: those of records still in T2/T3 on their other
    signals (a paper that cites VASP lifts a record to T1; a materials-field paper lifts T3 to
    T2). T1 records are already selected and T0 ones skipped, so their papers are left out
    unless ``include_t1``."""
    out: set[str] = set()
    for rec in iter_census(census_path):
        if excl.status(rec) or (rec.get("metadata") or {}).get("access_right") not in (
                None, "open"):
            continue
        sig = record_signals(rec, seeds, citing, epmc)
        tier, _ = tier_of(sig)
        if tier == "T0" or (tier == "T1" and not include_t1):
            continue
        out.update(sig["paper_dois"])
        out.update(sig["citing_dois"])
    return out


def _compact(sig: dict[str, Any]) -> dict[str, Any]:
    """Signals worth keeping in scored.jsonl (non-empty ones)."""
    return {k: v for k, v in sig.items() if v not in (None, [], "", False, 0) or k == "desc_len"}


def epmc_coverage(epmc: dict[str, set[str]], versions: dict[str, str], excl: Exclusions,
                  key_tier: dict[str, str], id_concept: dict[str, str]) -> dict[str, int]:
    """Where the Zenodo records named in VASP papers' full texts (Europe PMC) stand, one count per
    record — every id (a version, the newest version, or the concept) is mapped to its concept
    through the WHOLE census (excluded records included) and then the resolved-versions table:
    already in the dataset, already evaluated by the keyword harvest, a census tier, or not in the
    census (no archive / loose VASP file, or unresolvable). The ``in_dataset`` share among the
    VASP-bearing ones is the keyword method's measured recall."""
    counts: Counter = Counter()
    seen: set[str] = set()
    for zid in epmc:
        concept = id_concept.get(zid) or versions.get(zid) or zid
        if concept in seen:
            continue
        seen.add(concept)
        if zid in excl.dataset_recids or concept in excl.dataset_concepts:
            counts["in_dataset"] += 1
        elif (zid in excl.evaluated_recids or concept in excl.evaluated_concepts) and \
                zid not in excl.recheck:
            counts["evaluated_by_keywords"] += 1
        elif key_tier.get(concept) or key_tier.get(zid):
            counts[f"census_{key_tier.get(concept) or key_tier.get(zid)}"] += 1
        else:
            counts["not_in_census"] += 1
    return dict(counts)


def score(census_path: str | Path, out_path: str | Path, *, excl: Exclusions, seeds: Seeds,
          citing: dict[str, set[str]] | None = None, epmc: dict[str, set[str]] | None = None,
          openalex: dict[str, dict[str, Any]] | None = None,
          versions: dict[str, str] | None = None,
          report_path: str | Path | None = None) -> dict[str, Any]:
    """Score every census record; write ``scored.jsonl`` + ``score_report.json``."""
    out = Path(out_path)
    report = Path(report_path) if report_path else out.with_name("score_report.json")
    counts: Counter = Counter()
    by_type: dict[str, Counter] = {}
    reasons: Counter = Counter()
    work: dict[str, Counter] = {t: Counter() for t in ("T1", "T2", "T3", "T0")}
    probes: dict[str, Any] = {}
    key_tier: dict[str, str] = {}
    id_concept: dict[str, str] = {}
    tmp_out = out.with_name(out.name + ".tmp")
    with tmp_out.open("w") as fh:
        for rec in iter_census(census_path):
            meta = rec.get("metadata") or {}
            rid = str(rec.get("id"))
            if epmc:
                concept = str(rec.get("conceptrecid") or rid)
                id_concept[rid] = id_concept[concept] = concept
            rt = meta.get("resource_type")
            rtype = str(rt.get("type") if isinstance(rt, dict) else rt)
            counts["census"] += 1
            ex = excl.status(rec)
            if rid in excl.recheck:
                counts["recheck_of_keyword_candidates"] += 1
            if ex:
                counts[f"excluded_{ex}"] += 1
                if rid in PROBE_RECIDS:
                    probes[rid] = {"excluded": ex}
                continue
            access = meta.get("access_right")
            lic = licence_id(meta)
            lic_cls = licence_class(lic)
            sig = record_signals(rec, seeds, citing, epmc, openalex)
            tier, why = tier_of(sig)
            if access not in (None, "open"):
                tier, why = "X", [f"access_{access}"]
            arch = archive_summary(rec.get("files") or [])
            row = {"recid": rid, "conceptrecid": str(rec.get("conceptrecid") or rid),
                   "created": (rec.get("created") or "")[:10], "resource_type": rtype,
                   "title": str(meta.get("title") or "")[:200], "licence": lic,
                   "licence_class": lic_cls, "licence_admitted": lic_cls in ADMITTED_LICENCES,
                   "tier": tier, "reasons": why, "signals": _compact(sig), **arch,
                   "n_files": len(rec.get("files") or []),
                   "bytes_total": sum(int(f.get("size") or 0) for f in rec.get("files") or [])}
            fh.write(json.dumps(row) + "\n")
            if epmc:
                key_tier[rid] = key_tier[row["conceptrecid"]] = tier
            counts[f"tier_{tier}"] += 1
            by_type.setdefault(rtype, Counter())[tier] += 1
            for r in why:
                reasons[f"{tier}:{r}"] += 1
            if tier in work:
                w = work[tier]
                w["records"] += 1
                w["zips"] += arch["n_zip"] + arch["n_aiida"]
                w["unpeekable"] += arch["n_unpeekable"]
                w["bytes_zip"] += arch["bytes_zip"]
                w["bytes_unpeekable"] += arch["bytes_unpeekable"]
                w[f"licence_{lic_cls}"] += 1
            if rid in PROBE_RECIDS:
                probes[rid] = {"tier": tier, "reasons": why, "licence_class": lic_cls}
    os.replace(tmp_out, out)
    summary = {"census": str(census_path), "out": str(out), "counts": dict(counts),
               "by_resource_type": {k: dict(v) for k, v in sorted(by_type.items())},
               "workload": {k: dict(v) for k, v in work.items()},
               "reasons": dict(reasons.most_common()), "probes": probes,
               "seeds": {"owners": len(seeds.owners), "orcids": len(seeds.orcids),
                         "names": len(seeds.names), "communities": len(seeds.communities)},
               "links": {"citing_records": len(citing or {}), "epmc_records": len(epmc or {}),
                         "openalex_dois": len(openalex or {}),
                         "resolved_versions": len(versions or {})},
               "epmc_coverage": epmc_coverage(epmc or {}, versions or {}, excl, key_tier,
                                              id_concept),
               "exclusions": {**dict(excl.stats), "recheck_sample": dict(list(
                   excl.recheck.items())[:20])}}
    tmp_rep = report.with_name(report.name + ".tmp")
    tmp_rep.write_text(json.dumps(summary, indent=1))
    os.replace(tmp_rep, report)
    logger.info("score: %s", {k: v for k, v in summary.items() if k in ("counts", "probes")})
    return summary


def iter_scored(path: str | Path, tiers: Iterable[str] | None = None) -> Iterator[dict[str, Any]]:
    want = set(tiers) if tiers else None
    for row in read_jsonl(path):
        if want is None or row.get("tier") in want:
            yield row


_GITHUB_TITLE = re.compile(r"^[\w.-]+/[\w.-]+:\s")


def is_github_snapshot(row: dict[str, Any]) -> bool:
    """A software record made by Zenodo's GitHub integration (``owner/repo: vX``)."""
    return row.get("resource_type") == "software" and bool(
        _GITHUB_TITLE.match(str(row.get("title") or "")))
