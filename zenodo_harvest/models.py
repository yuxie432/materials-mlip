"""Data model for a harvested Zenodo record and the VASP file-signal classifier.

A *candidate* is a slimmed-down view of a Zenodo record carrying exactly the
fields we need to (a) decide whether it is worth downloading and (b) preserve
provenance. It is serialised one-JSON-object-per-line (JSONL) so manifests stream
and append cheaply — the standard way to accumulate millions of records without
loading them all into memory. See docs/DESIGN.md.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# File-name signals. Zenodo indexes file *extensions* but the DFT outputs almost
# always sit inside an archive, so we classify on the full listing, not on a
# single extension filter.
# ---------------------------------------------------------------------------

# Canonical VASP output/input filenames (case-insensitive, allow pre/suffixes,
# and a trailing compression extension like .gz/.bz2/.xz/.zst).
VASP_STEMS = [
    "vasprun", "outcar", "vaspout", "oszicar", "poscar", "contcar", "incar",
    "kpoints", "potcar", "xdatcar", "doscar", "eigenval", "procar", "ibzkpt",
]
# The two we actually parse (mentor: vasprun.xml / vaspout.h5 + OUTCAR).
VASP_PRIMARY = {"vasprun", "outcar", "vaspout"}

_COMPRESS = r"(?:\.(?:gz|bz2|xz|zst|lz4))?"
# A canonical stem as a *word* (start of basename or after a separator), then any
# number of separator-delimited suffix tokens and/or an extension, plus optional
# compression. The trailing ``(?:[._\-]\w+)*`` is what lets us also match numbered
# / suffixed variants VASP or uploaders emit — ``vasprun_1.xml``, ``OUTCAR_final``,
# ``vasprun.relax.xml`` — instead of dropping those records at triage (recall).
# Over-matching (e.g. ``poscar_notes.txt``) is safe: POSCAR is not a primary output
# so it never seeds a calc unit on its own, and pymatgen parse is the precision gate.
_VASP_RE = re.compile(
    r"(?:^|[/_.\-])(" + "|".join(VASP_STEMS) + r")(?:[._\-]\w+)*" + _COMPRESS + r"$",
    re.IGNORECASE,
)

ARCHIVE_EXTS = {
    ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar", ".tbz2", ".zst",
    ".tar.gz", ".tar.bz2", ".tar.xz",  # _ext() returns these doubles verbatim
}
# Already-processed atomistic formats — usable, but provenance/quality is the
# uploader's, not ours; tag rather than treat as raw VASP.
PROCESSED_EXTS = {".extxyz", ".xyz", ".traj", ".aselmdb", ".lmdb", ".h5", ".hdf5", ".json", ".cif"}

# Category ranking (higher = more directly usable as raw VASP).
CATEGORY_RANK = {
    "vasp_direct": 4,      # recognisable VASP output files exposed directly
    "archive": 3,          # archive(s) present; contents unknown until peeked
    "processed_atomistic": 2,  # extxyz/xyz/traj/etc. already extracted by uploader
    "unlikely": 0,
}


def _ext(name: str) -> str:
    name = name.lower()
    for double in (".tar.gz", ".tar.bz2", ".tar.xz"):
        if name.endswith(double):
            return double
    dot = name.rfind(".")
    return name[dot:] if dot != -1 else ""


def classify_files(files: list[dict[str, Any]]) -> dict[str, Any]:
    """Classify a record's file listing for VASP likelihood.

    ``files`` are Zenodo file objects with at least ``key`` (name) and ``size``.
    Returns category, matched VASP filenames, archive names, and human-readable
    signals. No network access — operates purely on the listing already returned
    by the search API.
    """
    vasp_hits: list[str] = []
    primary_hits: list[str] = []
    archives: list[str] = []
    processed: list[str] = []
    for f in files:
        name = f.get("key", "")
        base = name.rsplit("/", 1)[-1]
        m = _VASP_RE.search(base)
        if m:
            vasp_hits.append(name)
            if m.group(1).lower() in VASP_PRIMARY:
                primary_hits.append(name)
        ext = _ext(base)
        if ext in ARCHIVE_EXTS:
            archives.append(name)
        if ext in PROCESSED_EXTS:
            processed.append(name)

    signals: list[str] = []
    if primary_hits:
        category = "vasp_direct"
        signals.append(f"exposes primary VASP outputs: {primary_hits[:3]}")
    elif vasp_hits:
        category = "vasp_direct"
        signals.append(f"exposes VASP files: {vasp_hits[:3]}")
    elif archives:
        category = "archive"
        signals.append(f"{len(archives)} archive(s), contents unknown (peek/download to confirm)")
    elif processed:
        category = "processed_atomistic"
        signals.append(f"pre-extracted atomistic files: {processed[:3]}")
    else:
        category = "unlikely"
        signals.append("no VASP/archive/atomistic files in listing")

    return {
        "category": category,
        "rank": CATEGORY_RANK[category],
        "vasp_files": vasp_hits,
        "primary_vasp_files": primary_hits,
        "archives": archives,
        "processed_files": processed,
        "signals": signals,
    }


# Metadata-text signals (weak, but useful to disambiguate archives).
_META_KEYWORDS = re.compile(
    r"\b(vasp|vasprun|outcar|incar|first[\- ]principles|density functional|"
    r"\bdft\b|ab[\- ]initio|projector augmented|paw|plane[\- ]wave)\b",
    re.IGNORECASE,
)


def metadata_signal(title: str, description: str, keywords: list[str]) -> list[str]:
    text = " ".join([title or "", description or "", " ".join(keywords or [])])
    return sorted({m.group(0).lower() for m in _META_KEYWORDS.finditer(text)})


@dataclass
class Candidate:
    """Provenance-preserving summary of one Zenodo record."""

    recid: str
    conceptrecid: str | None
    doi: str | None
    conceptdoi: str | None
    title: str
    creators: list[str]
    publication_date: str | None
    created: str | None
    keywords: list[str]
    license: str | None
    resource_type: str | None
    access_right: str | None  # "open"/"embargoed"/"restricted"/"closed" (may be absent)
    zenodo_url: str
    files_total: int
    bytes_total: int
    files: list[dict[str, Any]]  # [{key, size, ext, checksum, download}]
    # classification / signals
    vasp_category: str
    vasp_rank: int
    vasp_files: list[str]
    primary_vasp_files: list[str]
    archives: list[str]
    processed_files: list[str]
    metadata_signals: list[str]
    signals: list[str]
    query: str | None = None
    retrieved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_record(cls, rec: dict[str, Any]) -> "Candidate":
        meta = rec.get("metadata", {})
        files = rec.get("files", []) or []
        norm_files = [
            {
                "key": f.get("key"),
                "size": f.get("size"),
                "ext": _ext(f.get("key", "")),
                "checksum": f.get("checksum"),
                "download": (f.get("links") or {}).get("self"),
            }
            for f in files
        ]
        fc = classify_files(files)
        rtype = meta.get("resource_type", {})
        rtype = rtype.get("type") if isinstance(rtype, dict) else rtype
        lic = meta.get("license", {})
        lic = lic.get("id") if isinstance(lic, dict) else lic
        access = meta.get("access_right")  # a plain string in the records API
        keywords = meta.get("keywords", []) or []
        return cls(
            recid=str(rec.get("id")),
            conceptrecid=str(rec.get("conceptrecid")) if rec.get("conceptrecid") else None,
            doi=rec.get("doi"),
            conceptdoi=rec.get("conceptdoi"),
            title=meta.get("title", ""),
            creators=[c.get("name", "") for c in (meta.get("creators") or [])],
            publication_date=meta.get("publication_date"),
            created=rec.get("created"),
            keywords=keywords,
            license=lic,
            resource_type=rtype,
            access_right=access,
            zenodo_url=(rec.get("links") or {}).get("self_html") or f"https://zenodo.org/records/{rec.get('id')}",
            files_total=len(norm_files),
            bytes_total=sum((f.get("size") or 0) for f in norm_files),
            files=norm_files,
            vasp_category=fc["category"],
            vasp_rank=fc["rank"],
            vasp_files=fc["vasp_files"],
            primary_vasp_files=fc["primary_vasp_files"],
            archives=fc["archives"],
            processed_files=fc["processed_files"],
            metadata_signals=metadata_signal(
                meta.get("title", ""), meta.get("description", ""), keywords
            ),
            signals=fc["signals"],
            query=rec.get("_query"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
