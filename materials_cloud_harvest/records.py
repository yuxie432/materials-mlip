"""Materials Cloud record → harvest candidate (the keep-list shape the shared stages read).

A candidate is a plain dict carrying (a) exactly the fields the SHARED Zenodo stages consume —
``recid`` + ``files[] = {key, size, ext, checksum, download}`` + the ``vasp_*`` classification
(triage/fetch/status) — and (b) a ready-made ``provenance`` block with ``source="materials_cloud"``
that :func:`zenodo_harvest.fetch._record_provenance` passes through verbatim, so the shared parser
namespaces calc_ids ``materials_cloud:<record_id>:…`` and every metadata record carries MC's
DOIs / licence / citations. Everything here is pure (no network) and unit-tested offline.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable

from zenodo_harvest.models import (
    _NO_LICENSE_IDS,
    CATEGORY_RANK,
    _ext,
    classify_files,
    metadata_signal,
)

from .client import BASE, content_url

SOURCE = "materials_cloud"
AIIDA_EXT = ".aiida"
# Tarballs whose compression the shared name sniffing does not know but ``tarfile`` reads (its xz
# opener auto-detects the legacy ``.lzma`` container): declared ``archive_kind="tar"`` explicitly.
EXTRA_TAR_SUFFIXES = (".tar.lzma", ".tlz")

# --- licence policy (user decision 2026-09-23: admit NC/NC-SA, drop ND / no-licence / MC non-open)
# Materials Cloud ids that are NOT open licences, although the shared blocklist gate
# (``models.is_reusable_license``, NC/ND token based) would pass them:
MC_NONOPEN_LICENCES = {
    # "Materials Cloud non-exclusive license to distribute v1.0" — arXiv-style: grants
    # Materials Cloud the right to distribute, grants third parties no reuse/derivative rights.
    "mcloud-ne-1.0": "distribution right granted to Materials Cloud only",
    # "Academic Software Licence" (autoWTE) — academic/non-commercial use only.
    "asl": "academic-use-only software licence",
}
LICENCE_POLICIES = ("nc-ok", "strict", "none")


def licence_id(meta: dict[str, Any]) -> str | None:
    """The record's licence id: the FIRST ``rights`` entry that has an ``id``.

    Some records append an id-less "License addendum" entry (free-text notes), which must not
    shadow the real licence."""
    for r in meta.get("rights") or []:
        if isinstance(r, dict) and r.get("id"):
            return str(r["id"])
    return None


def licence_verdict(lic: str | None, policy: str = "nc-ok") -> tuple[bool, str]:
    """``(keep, reason)`` for a licence id under ``policy``.

    * ``nc-ok`` (default, user decision): keep CC0/CC-BY/CC-BY-SA/permissive software licences
      AND CC-BY-NC/-NC-SA (matching the Zenodo dataset after its NC expansion); drop ND, no
      licence, and the MC non-open ids (:data:`MC_NONOPEN_LICENCES`).
    * ``strict``: the Zenodo default gate — additionally drop NC.
    * ``none``: keep everything (licence stays a provenance tag).
    """
    if policy not in LICENCE_POLICIES:
        raise ValueError(f"unknown licence policy {policy!r}; choose from {LICENCE_POLICIES}")
    if policy == "none":
        return True, "no_gate"
    if lic is None:
        return False, "no_licence"
    norm = lic.strip().lower()
    if norm in _NO_LICENSE_IDS:
        return False, "no_licence"
    if norm in MC_NONOPEN_LICENCES:
        return False, "mc_nonopen_licence"
    tokens = set(re.split(r"[-_.\s]+", norm))
    if "nd" in tokens:
        return False, "no_derivatives"
    if policy == "strict" and "nc" in tokens:
        return False, "non_commercial"
    return True, "ok"


# --- text signals ---------------------------------------------------------------------------
# VASP named in the record's own text. Author affiliations are deliberately NOT searched: the
# server's ``q=VASP`` matched the Bosoni ACWF record only via "VASP Software GmbH" — a record
# co-authored by the VASP developers can be about anything (that record is handled by evidence).
_VASP_MENTION_RE = re.compile(r"\bvasp\b|vasprun|\boutcar\b|vienna ab[\s-]*initio", re.IGNORECASE)
_OTHER_CODES = {
    "quantum_espresso": r"quantum[\s-]*espresso|\bpw\.x\b|pwscf|aiida[\s-]quantumespresso",
    "cp2k": r"\bcp2k\b", "siesta": r"\bsiesta\b", "fhi-aims": r"fhi[\s-]?aims",
    "castep": r"\bcastep\b", "abinit": r"\babinit\b", "gpaw": r"\bgpaw\b",
    "orca": r"\borca\b", "lammps": r"\blammps\b", "yambo": r"\byambo\b",
    "wannier90": r"wannier90", "fleur": r"\bfleur\b", "wien2k": r"wien2k", "bigdft": r"bigdft",
}
_OTHER_CODES_RE = {k: re.compile(v, re.IGNORECASE) for k, v in _OTHER_CODES.items()}


def record_text(meta: dict[str, Any]) -> str:
    """Title + description + subject keywords — the record's own descriptive text."""
    subjects = " ".join(str(s.get("subject", "")) for s in meta.get("subjects") or []
                        if isinstance(s, dict))
    return " ".join([str(meta.get("title") or ""), str(meta.get("description") or ""), subjects])


def vasp_mentioned(meta: dict[str, Any]) -> bool:
    return bool(_VASP_MENTION_RE.search(record_text(meta)))


def other_codes(meta: dict[str, Any]) -> list[str]:
    text = record_text(meta)
    return sorted(k for k, rx in _OTHER_CODES_RE.items() if rx.search(text))


# --- identifiers ------------------------------------------------------------------------------
_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"'<>,;]+", re.IGNORECASE)
# An MC record's stable short id ("mcid", e.g. 2020.0006) as it appears in its version DOI
# (10.24435/materialscloud:2020.0006/v1) and in old-style archive URLs (…/record/2020.0006/v1).
_MCID_RE = re.compile(r"materialscloud(?::|\.org/record/)(\d{4}\.\d{2,6})", re.IGNORECASE)


def normalize_doi(text: str | None) -> str | None:
    """A bare, lower-cased DOI (``10.xxxx/…``) found in ``text``, else None."""
    if not text:
        return None
    m = _DOI_RE.search(str(text).strip())
    return m.group(0).rstrip(".)]").lower() if m else None


def mcid_base(text: str | None) -> str | None:
    """The version-free MC short id (``2020.0006``) referenced by ``text`` (a DOI/URL/mcid)."""
    if not text:
        return None
    s = str(text)
    m = _MCID_RE.search(s) or re.fullmatch(r"(\d{4}\.\d{2,6})(?:/v\d+)?", s.strip())
    return m.group(1) if m else None


def related_identifiers(meta: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for ri in meta.get("related_identifiers") or []:
        if not isinstance(ri, dict) or not ri.get("identifier"):
            continue
        out.append({"identifier": ri.get("identifier"), "scheme": ri.get("scheme"),
                    "relation": (ri.get("relation_type") or {}).get("id"),
                    "resource_type": (ri.get("resource_type") or {}).get("id")})
    return out


def mc_references(rec: dict[str, Any]) -> list[dict[str, Any]]:
    """The citations MC attaches to a record (``custom_fields.mc_references``) — provenance."""
    out = []
    for ref in (rec.get("custom_fields") or {}).get("mc_references") or []:
        if not isinstance(ref, dict):
            continue
        link = ref.get("ref_link") or {}
        out.append({"citation": ref.get("ref_citation"), "identifier": link.get("ref_identifier"),
                    "scheme": link.get("ref_scheme"), "resource_type": ref.get("ref_resource_type")})
    return out


def linked_dois(cand: dict[str, Any]) -> set[str]:
    """Every DOI a candidate links to (related identifiers + MC references), normalised."""
    dois: set[str] = set()
    for ri in cand.get("related_identifiers") or []:
        d = normalize_doi(ri.get("identifier"))
        if d:
            dois.add(d)
    for ref in cand.get("references") or []:
        d = normalize_doi(ref.get("identifier"))
        if d:
            dois.add(d)
    return dois


# --- files ------------------------------------------------------------------------------------

def normalize_files(rec: dict[str, Any], base: str = BASE) -> list[dict[str, Any]]:
    """The record's file list in the shared keep-list shape, sorted by key (deterministic).

    MC's ``files.entries`` is a dict keyed by filename (a list is accepted too). The download URL
    is built from the record id + key (the listing carries none); ``ext`` uses the shared
    ``models._ext`` (``.tar.gz`` etc. as doubles) so triage/fetch classify exactly as for Zenodo.
    """
    recid = str(rec.get("id"))
    ents = (rec.get("files") or {}).get("entries") or {}
    items = list(ents.items()) if isinstance(ents, dict) else [(e.get("key"), e) for e in ents]
    out = []
    for name, e in items:
        key = str((e or {}).get("key") or name or "")
        if not key:
            continue
        f = {"key": key, "size": int((e or {}).get("size") or 0), "ext": _ext(key),
             "checksum": (e or {}).get("checksum"), "download": content_url(recid, key, base),
             "mimetype": (e or {}).get("mimetype")}
        if key.lower().endswith(EXTRA_TAR_SUFFIXES):
            f["archive_kind"] = "tar"
        out.append(f)
    return sorted(out, key=lambda f: f["key"])


def classify_mc_files(files: list[dict[str, Any]]) -> dict[str, Any]:
    """The shared VASP file classifier, made aware of AiiDA export archives.

    ``*.aiida`` is not a filename the shared classifier knows, so a record whose data lives only
    in AiiDA exports (e.g. the Bosoni ACWF verification record) would rank ``unlikely`` and never
    reach triage. An AiiDA export is a zip archive (legacy 0.x) or a sqlite+zip bundle (≥2.0),
    i.e. an archive whose contents only a peek can reveal — so it is ranked like one."""
    fc = classify_files(files)
    aiida = [f["key"] for f in files if str(f.get("key", "")).lower().endswith(AIIDA_EXT)]
    declared = [f["key"] for f in files if f.get("archive_kind") and f["key"] not in aiida]
    if aiida or declared:
        fc["archives"] = sorted(set(fc["archives"]) | set(aiida) | set(declared))
        if fc["rank"] < CATEGORY_RANK["archive"]:
            fc["category"], fc["rank"] = "archive", CATEGORY_RANK["archive"]
            fc["signals"] = [s for s in fc["signals"] if "no VASP/archive" not in s]
        if aiida:
            fc["signals"].append(f"{len(aiida)} AiiDA export(s), contents unknown (peek to confirm)")
        if declared:
            fc["signals"].append(f"{len(declared)} other archive(s) ({', '.join(declared[:3])})")
    return fc


# --- the candidate ----------------------------------------------------------------------------

def _access_right(rec: dict[str, Any]) -> str:
    acc = rec.get("access") or {}
    if (acc.get("record") == "public" and acc.get("files") == "public"
            and not (acc.get("embargo") or {}).get("active")):
        return "open"
    return str(acc.get("status") or acc.get("files") or "restricted")


def record_to_candidate(rec: dict[str, Any], base: str = BASE) -> dict[str, Any]:
    """Normalise one API record into a harvest candidate (see the module docstring)."""
    meta = rec.get("metadata") or {}
    parent = rec.get("parent") or {}
    pids = rec.get("pids") or {}
    recid = str(rec.get("id"))
    files = normalize_files(rec, base)
    fc = classify_mc_files(files)
    lic = licence_id(meta)
    creators = [((c.get("person_or_org") or {}).get("name") or "") for c in meta.get("creators") or []
                if isinstance(c, dict)]
    keywords = [str(s.get("subject")) for s in meta.get("subjects") or []
                if isinstance(s, dict) and s.get("subject")]
    rtype = (meta.get("resource_type") or {}).get("id")
    doi = (pids.get("doi") or {}).get("identifier")
    conceptdoi = ((parent.get("pids") or {}).get("doi") or {}).get("identifier")
    mcid = (pids.get("mcid") or {}).get("identifier")
    url = (rec.get("links") or {}).get("self_html") or f"{base}/records/{recid}"
    rel = related_identifiers(meta)
    refs = mc_references(rec)
    provenance = {
        "source": SOURCE,
        "record_id": recid,
        "conceptrecid": parent.get("id"),
        "doi": doi,
        "conceptdoi": conceptdoi,
        "mcid": mcid,
        "url": url,
        "title": meta.get("title"),
        "creators": creators,
        "license": lic,
        "resource_type": rtype,
        "publication_date": meta.get("publication_date"),
        "keywords": keywords,
        "references": [r for r in refs if r.get("citation") or r.get("identifier")],
        "related_identifiers": rel,
    }
    return {
        "recid": recid,
        "record_id": recid,
        "conceptrecid": parent.get("id"),
        "doi": doi,
        "conceptdoi": conceptdoi,
        "mcid": mcid,
        "title": meta.get("title") or "",
        "creators": creators,
        "publication_date": meta.get("publication_date"),
        "created": rec.get("created"),
        "updated": rec.get("updated"),
        "keywords": keywords,
        "license": lic,
        "resource_type": rtype,
        "access_right": _access_right(rec),
        "url": url,
        "version_index": (rec.get("versions") or {}).get("index"),
        "files_total": len(files),
        "files_count_declared": (rec.get("files") or {}).get("count"),
        "bytes_total": sum(f["size"] for f in files),
        "files": files,
        "vasp_category": fc["category"],
        "vasp_rank": fc["rank"],
        "vasp_files": fc["vasp_files"],
        "primary_vasp_files": fc["primary_vasp_files"],
        "archives": fc["archives"],
        "processed_files": fc["processed_files"],
        "signals": fc["signals"],
        "metadata_signals": metadata_signal(meta.get("title", ""), meta.get("description", ""),
                                            keywords),
        "vasp_mention": vasp_mentioned(meta),
        "other_codes": other_codes(meta),
        "related_identifiers": rel,
        "references": refs,
        "provenance": provenance,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }


# --- overlap with the other harvested sources (FLAG, never auto-drop) -------------------------
_WORD_RE = re.compile(r"[a-z0-9]+")
_STOP = {"the", "of", "and", "in", "for", "a", "an", "on", "to", "with", "by", "from", "via",
         "data", "dataset", "datasets", "supporting", "information", "study", "at", "as", "its"}


def title_tokens(title: str | None) -> frozenset[str]:
    return frozenset(w for w in _WORD_RE.findall(str(title or "").lower())
                     if w not in _STOP and len(w) > 2)


def title_similarity(a: Iterable[str], b: Iterable[str]) -> float:
    """Jaccard similarity of two title token sets (0 when either is empty)."""
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)
