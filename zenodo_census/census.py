"""Stage 0' — the census: every Zenodo record that could hold VASP output.

Keyword discovery (``zenodo_harvest.discover``) finds a record only when its metadata TEXT matches
a DFT/VASP query, and measured on the live index (2026-09-25) that misses real VASP deposits for
three reasons beyond "the search never looks inside archives":

* sparse metadata — ``13888307`` (19.6 GB of VASP) says only "Accompanying data … article link";
* an HTML ``&nbsp;`` glues words into one token — ``4541602`` says "ab initio&nbsp;defect", so
  neither "ab initio" nor "defect" matches;
* quoted phrases are not stemmed — ``"potential energy surface"`` misses "…surfaces", and the same
  holds for most multi-word phrases in ``discover.DEFAULT_QUERIES``.

The census therefore asks Zenodo for records by what they HOLD, not what they say: InvenioRDM field
syntax selects every record with an archive (``files.entries.ext``) or a loose VASP primary
(``files.entries.key`` wildcards). That is 583,410 + 33 records (2026-09-25) across ALL resource
types; at the token's page size of 100 it is ~5.8k search requests ≈ 3.4 h under the 30 req/min
search cap — one batch job — after which every text/author/community/paper check runs offline.

Output: ``census.jsonl``, one slimmed hit per line (everything :meth:`Candidate.from_record` and the
scorer read — including the depositor account ``owners``, ORCIDs, communities, related identifiers
and the description), ``census.jsonl.windows.jsonl``, one sentinel per completed leaf ``created``
window, and ``census.jsonl.bounds.json``, the ``created`` range fixed by the FIRST run — the leaf
windows are bisections of that range, so a resume (the same day or later) must reuse it or none of
its windows would match. The census is therefore a snapshot of that range; ``fresh`` starts a new one. Lines are appended as they
arrive; a window re-paged after a crash, or a record whose new version lands in a later window, can
repeat a concept — :func:`iter_census` yields the deduplicated view (newest version per concept,
the same rule as discovery). ``census.jsonl.poison.jsonl`` lists the records Zenodo's default JSON
serializer cannot return (HTTP 500), which :class:`CensusClient` reads from the native serializer
instead (:func:`legacy_from_native`).
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import quote

import requests

from zenodo_harvest.client import DEFAULT_PAGE_SIZE, MAX_SEARCH_WINDOW, ZenodoClient
from zenodo_harvest.manifest import JsonlWriter

logger = logging.getLogger(__name__)

# ``files.entries.ext`` is the lower-cased last extension (``data.tar.gz`` -> ``gz``); an
# upper-case query matches nothing (verified), so lower case covers ``DATA.ZIP`` too.
ARCHIVE_EXTS = ("zip", "gz", "tgz", "tar", "7z", "rar", "xz", "bz2", "zst", "tbz2", "tbz",
                "txz", "tzst", "lzma", "aiida")
# Loose VASP primaries (no archive): wildcard key queries are case-sensitive, hence the variants.
LOOSE_KEYS = ("*OUTCAR*", "*outcar*", "*Outcar*", "*vasprun*", "*Vasprun*", "*VASPRUN*",
              "*vaspout*")
CENSUS_QUERY = (f"files.entries.ext:({' OR '.join(ARCHIVE_EXTS)}) "
                f"OR files.entries.key:({' OR '.join(LOOSE_KEYS)})")
# Zenodo caps search pages at 25 anonymously (size=26 -> HTTP 400) and 100 with a token.
PAGE_SIZE_TOKEN, PAGE_SIZE_ANON = 100, 25
# 30 req/min on the search endpoint, measured between request STARTS: 2.2 s (~27/min) leaves
# margin for network jitter.
SEARCH_INTERVAL = 2.2
DESC_MAX = 20_000          # characters of description kept (plenty for scoring; bounds the file)
NOTES_MAX = 5_000
MAX_REFERENCES = 50
# InvenioRDM's own serializer. Zenodo's default ("legacy") JSON serializer crashes with HTTP 500 on
# some records — measured 2026-09-26: 20797668 (an ACTRIS record whose only location is a Polygon
# geometry) fails in every search page that holds it and at /api/records/20797668, while this
# serializer returns it; the first census run died on it after 3 h.
NATIVE_ACCEPT = "application/vnd.inveniordm.v1+json"
ISOLATE_RETRIES = 1        # a failing sub-page is split further rather than retried at length
NATIVE_RETRIES = 3
OUTAGE_PATIENCE = 3600.0   # seconds of failed health probes before the census stops (resumable)
OUTAGE_DELAY = 60.0        # first wait between health probes; doubles, capped at OUTAGE_DELAY_MAX
OUTAGE_DELAY_MAX = 600.0
MAX_PAGE_RESTARTS = 5      # outages met while isolating one page before the census stops


class ZenodoOutage(RuntimeError):
    """Zenodo search stayed down past the patience: the census stops (resubmit to resume) rather
    than mistake an outage for records it cannot serialize."""


class _Restart(Exception):
    """An outage began while a page was being isolated: redo that page from the start."""


def _transient(exc: BaseException) -> bool:
    """A server or network failure (worth isolating or waiting out), not a 4xx of ours."""
    if isinstance(exc, requests.HTTPError):
        return exc.response is None or exc.response.status_code >= 500
    return isinstance(exc, requests.RequestException)


def _brief(exc: BaseException) -> str:
    resp = getattr(exc, "response", None)
    if resp is not None and getattr(resp, "status_code", None):
        return f"HTTP {resp.status_code}"
    return type(exc).__name__


class CensusClient(ZenodoClient):
    """:class:`ZenodoClient` paced on request STARTS rather than from the end of each response.

    The shared client waits ``min_interval`` after a response returns, so a page that takes 2-3 s
    to serve (100 records with file listings) costs ~5 s per request — twice the time the 30/min
    cap allows. The census is 5.8k pages, so pacing the starts halves it (3.4 h instead of ~7 h)
    while staying under the same cap; 429s are still honoured by the shared retry loop."""

    def __init__(self, base: str = "https://zenodo.org", token: str | None = None,
                 min_interval: float = SEARCH_INTERVAL, session: Any = None):
        super().__init__(base=base, token=token, min_interval=min_interval, session=session)
        self._last_start = 0.0
        self.outage_patience = OUTAGE_PATIENCE
        self.outage_delay = OUTAGE_DELAY
        # receives one event per record the default serializer cannot return (see iter_window)
        self.on_poison: Callable[[dict[str, Any]], None] | None = None

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_start)
        if wait > 0:
            time.sleep(wait)
        self._last_start = time.monotonic()

    # -- serializer-proof paging -------------------------------------------------------------

    def _search(self, query: str, page: int, size: int, sort: str = "newest",
                extra: dict[str, Any] | None = None, *, native: bool = False,
                retries: int = 5) -> dict[str, Any]:
        params = {"q": query, "page": page, "size": size, "sort": sort, **(extra or {})}
        return self._get("/api/records", params, max_retries=retries,
                         headers={"Accept": NATIVE_ACCEPT} if native else None)

    def _await_healthy(self, query: str, extra: dict[str, Any] | None
                       ) -> tuple[dict[str, Any], float]:
        """Probe until Zenodo answers ``query`` — ``(response, seconds waited)``. The probe asks
        for a page past the end (no hit is serialized, so no record can break it; ``hits.total``
        still comes back) through the native serializer. Raises :class:`ZenodoOutage` once
        ``outage_patience`` is spent."""
        waited, delay = 0.0, self.outage_delay
        while True:
            try:
                return self._search(query, MAX_SEARCH_WINDOW, 1, extra=extra, native=True,
                                    retries=2), waited
            except requests.RequestException as exc:
                if not _transient(exc):
                    raise
                if waited >= self.outage_patience:
                    raise ZenodoOutage(f"Zenodo search failing for {waited / 60:.0f} min "
                                       f"({_brief(exc)}); resubmit to resume") from exc
                logger.warning("census: Zenodo search failing (%s); waiting %.0f s",
                               _brief(exc), delay)
                time.sleep(delay)
                waited += delay
                delay = min(delay * 2, OUTAGE_DELAY_MAX)

    def search_page(self, query: str, page: int = 1, size: int = DEFAULT_PAGE_SIZE,
                    sort: str = "newest", extra: dict[str, Any] | None = None) -> dict:
        """The shared page (used by :func:`links.resolve_versions`), but a page the default
        serializer fails on comes back through the native one, every hit converted by
        :func:`legacy_from_native` (exact for the fields the census reads); outages are waited
        out, never mistaken for such a page."""
        while True:
            try:
                return self._search(query, page, size, sort, extra)
            except requests.RequestException as exc:
                if not _transient(exc):
                    raise
                err = _brief(exc)
            if self._await_healthy(query, extra)[1]:
                continue
            logger.warning("census: page %d of %r fails (%s) while Zenodo answers — reading it "
                           "through the native serializer", page, query, err)
            data = self._search(query, page, size, sort, extra, native=True,
                                retries=NATIVE_RETRIES)
            data["hits"]["hits"] = [legacy_from_native(h, self.base) for h in data["hits"]["hits"]]
            return data

    def count(self, query: str, extra: dict[str, Any] | None = None) -> int:
        """The shared count serializes a window's newest hit, so a window whose newest record is
        one the default serializer breaks on is counted by the health probe instead."""
        try:
            return super().count(query, extra)
        except requests.RequestException as exc:
            if not _transient(exc):
                raise
            logger.warning("census: count failed (%s); counting without serializing a hit",
                           _brief(exc))
        return int(self._await_healthy(query, extra)[0]["hits"]["total"])

    def iter_window(self, query: str, size: int = DEFAULT_PAGE_SIZE, sort: str = "newest",
                    extra: dict[str, Any] | None = None) -> Iterator[dict]:
        """:meth:`ZenodoClient.iter_window`, but a page that fails persistently while Zenodo
        answers is split into aligned sub-pages down to single records, and a single record the
        default serializer still cannot return is taken from the native one
        (:func:`legacy_from_native`) — or, if that fails too, reported as unresolved. Every such
        record goes to ``on_poison``; none is dropped silently. An outage is waited out, never
        mistaken for broken records."""
        total: int | None = None
        yielded, page = 0, 1
        while True:
            hits, t, served = self._page(query, page, size, sort, extra)
            if total is None:
                total = t if t is not None else self.count(query, extra)
                if total > MAX_SEARCH_WINDOW:
                    logger.warning("query %r has %d hits (> %d window); results will be "
                                   "truncated", query, total, MAX_SEARCH_WINDOW)
            for rec in hits:
                yield rec
                yielded += 1
                if yielded >= MAX_SEARCH_WINDOW:
                    return
            if served == 0 or page * size >= min(total, MAX_SEARCH_WINDOW):
                return
            page += 1

    def _page(self, query: str, page: int, size: int, sort: str,
              extra: dict[str, Any] | None) -> tuple[list[dict], int | None, int]:
        """One page: ``(hits, total or None, records the server holds at those offsets)``."""
        restarts = 0
        while True:
            try:
                data = self._search(query, page, size, sort, extra)
                hits = data["hits"]["hits"]
                return hits, int(data["hits"]["total"]), len(hits)
            except requests.RequestException as exc:
                if not _transient(exc):
                    raise
                err = _brief(exc)
            if self._await_healthy(query, extra)[1]:
                continue                    # Zenodo was down and is back: retry the page as is
            logger.warning("census: page %d (size %d) fails (%s) while Zenodo answers — "
                           "isolating the record(s) it cannot serialize", page, size, err)
            try:
                return self._isolate(query, (page - 1) * size, size, sort, extra)
            except _Restart:
                restarts += 1
                if restarts > MAX_PAGE_RESTARTS:
                    raise ZenodoOutage(f"repeated outages while isolating page {page} of "
                                       f"{query!r}; resubmit to resume") from None

    def _try_page(self, query: str, page: int, size: int, sort: str,
                  extra: dict[str, Any] | None) -> tuple[list[dict], int | None, int] | None:
        try:
            data = self._search(query, page, size, sort, extra, retries=ISOLATE_RETRIES)
        except requests.RequestException as exc:
            if not _transient(exc):
                raise
            return None
        hits = data["hits"]["hits"]
        return hits, int(data["hits"]["total"]), len(hits)

    def _isolate(self, query: str, offset: int, n: int, sort: str,
                 extra: dict[str, Any] | None) -> tuple[list[dict], int | None, int]:
        """Records ``offset .. offset+n-1`` of a page the server cannot serialize whole, fetched
        as aligned sub-pages (100 -> 10 -> 1; ``offset`` is a multiple of ``n``)."""
        c = n // 10 if n > 10 and n % 10 == 0 else 1
        hits: list[dict] = []
        total: int | None = None
        served = 0
        for off in range(offset, offset + n, c):
            sub = (self._single(query, off, sort, extra) if c == 1
                   else self._try_page(query, off // c + 1, c, sort, extra))
            if sub is None:
                sub = self._isolate(query, off, c, sort, extra)
            hits.extend(sub[0])
            total = sub[1] if total is None else total
            served += sub[2]
            if sub[2] < c:                  # past the window's last record
                break
        return hits, total, served

    def _single(self, query: str, offset: int, sort: str,
                extra: dict[str, Any] | None) -> tuple[list[dict], int | None, int]:
        sub = self._try_page(query, offset + 1, 1, sort, extra)
        if sub is not None:
            return sub
        where = {"query": query, "sort": sort, "offset": offset}
        err = ""
        for _ in (1, 2):
            try:
                data = self._search(query, offset + 1, 1, sort, extra, native=True,
                                    retries=NATIVE_RETRIES)
                break
            except requests.RequestException as exc:
                if not _transient(exc):
                    raise
                err = _brief(exc)
            if self._await_healthy(query, extra)[1]:
                raise _Restart          # an outage, not this record: redo the page when back
        else:
            self._poison({"status": "unresolved", **where,
                          "error": f"both serializers fail ({err})"})
            return [], None, 1
        total = int(data["hits"]["total"])
        if not data["hits"]["hits"]:
            return [], total, 0
        nat = data["hits"]["hits"][0]
        try:
            rec = legacy_from_native(nat, self.base)
        except Exception as exc:  # noqa: BLE001 — one odd record must not stop the census
            self._poison({"status": "unresolved", **where, "id": nat.get("id"),
                          "error": f"native record not convertible: {exc!r}", "native": nat})
            return [], total, 1
        self._poison({"status": "converted", **where, "id": rec["id"], "native": nat})
        return [rec], total, 1

    def _poison(self, event: dict[str, Any]) -> None:
        logger.warning("census: record at offset %d of %r %s%s", event["offset"], event["query"],
                       event["status"], f" (id {event['id']})" if event.get("id") else "")
        if self.on_poison is not None:
            self.on_poison(event)


# ---------------------------------------------------------------------------
# native (InvenioRDM) record -> the default serializer's shape
# ---------------------------------------------------------------------------

# Licence ids the default serializer renames (verified on live pairs, 2026-09-26); the rest pass
# through — licence_class / is_reusable_license work on tokens, so both spellings classify alike.
_LEGACY_LICENCE = {"cc0-1.0": "cc-zero", "mit": "mit-license"}
_LEGACY_RELATION = {r.lower(): r[0].lower() + r[1:] for r in (
    "IsCitedBy Cites IsSupplementTo IsSupplementedBy IsContinuedBy Continues IsDescribedBy "
    "Describes HasMetadata IsMetadataFor HasVersion IsVersionOf IsNewVersionOf "
    "IsPreviousVersionOf IsPartOf HasPart IsPublishedIn IsReferencedBy References "
    "IsDocumentedBy Documents IsCompiledBy Compiles IsVariantFormOf IsOriginalFormOf "
    "IsIdenticalTo IsReviewedBy Reviews IsDerivedFrom IsSourceOf IsRequiredBy Requires "
    "IsObsoletedBy Obsoletes IsCollectedBy Collects IsTranslationOf HasTranslation").split()}
_LEGACY_CONTRIBUTOR = {t.lower(): t for t in (
    "ContactPerson DataCollector DataCurator DataManager Distributor Editor HostingInstitution "
    "Producer ProjectLeader ProjectManager ProjectMember RegistrationAgency "
    "RegistrationAuthority RelatedPerson Researcher ResearchGroup RightsHolder Supervisor "
    "Sponsor WorkPackageLeader Other").split()}
_LEGACY_ACCESS = {"open": "open", "embargoed": "embargoed", "restricted": "restricted",
                  "metadata-only": "closed"}


def _legacy_person(p: dict[str, Any], contributor: bool = False) -> dict[str, Any]:
    po = p.get("person_or_org") or {}
    name = po.get("name") or ", ".join(x for x in (po.get("family_name"),
                                                   po.get("given_name")) if x)
    out: dict[str, Any] = {"name": name}
    aff = next((a["name"] for a in p.get("affiliations") or [] if a.get("name")), None)
    if aff:
        out["affiliation"] = aff        # the default serializer keeps the first affiliation
    orcid = next((i.get("identifier") for i in po.get("identifiers") or []
                  if i.get("scheme") == "orcid"), None)
    if orcid:
        out["orcid"] = orcid
    role = (p.get("role") or {}).get("id")
    if contributor and role:
        out["type"] = _LEGACY_CONTRIBUTOR.get(role, role)
    return out


def legacy_from_native(hit: dict[str, Any], base: str = "https://zenodo.org") -> dict[str, Any]:
    """A native InvenioRDM search hit rewritten into the default serializer's shape — every
    field :func:`slim_hit` and :meth:`Candidate.from_record` read — for the records the default
    serializer cannot return. Checked field by field against both serializations of the same
    live records (2026-09-26); tagged ``_serializer: "inveniordm"`` so a census line made this
    way stays traceable."""
    m = hit.get("metadata") or {}
    parent = hit.get("parent") or {}
    rid = str(hit.get("id"))
    rt = m.get("resource_type") or {}
    typ, _, sub = str(rt.get("id") or "").partition("-")
    resource_type: dict[str, Any] = {"title": (rt.get("title") or {}).get("en"), "type": typ}
    if sub:
        resource_type["subtype"] = sub
    lic = next((r["id"] for r in m.get("rights") or [] if r.get("id")), None)
    access = hit.get("access") or {}
    status = access.get("status") or ("embargoed" if (access.get("embargo") or {}).get("active")
                                      else "open" if access.get("files") == "public"
                                      else "restricted")
    subjects = m.get("subjects") or []
    related = []
    for r in m.get("related_identifiers") or []:
        rel = str((r.get("relation_type") or {}).get("id") or "")
        entry = {"identifier": r.get("identifier"), "relation": _LEGACY_RELATION.get(rel, rel),
                 "scheme": r.get("scheme")}
        if (r.get("resource_type") or {}).get("id"):
            entry["resource_type"] = r["resource_type"]["id"]
        related.append(entry)
    notes = next((d.get("description") for d in m.get("additional_descriptions") or []
                  if (d.get("type") or {}).get("id") == "notes"), None)
    meta: dict[str, Any] = {
        "title": m.get("title") or "",
        "description": m.get("description") or "",
        "keywords": [s["subject"] for s in subjects if s.get("subject") and not s.get("id")],
        "subjects": [{"term": s.get("subject"), "identifier": s["id"], "scheme": s.get("scheme")}
                     for s in subjects if s.get("id")],
        "resource_type": resource_type,
        "license": {"id": _LEGACY_LICENCE.get(lic, lic)} if lic else None,
        "access_right": _LEGACY_ACCESS.get(status, status),
        "publication_date": m.get("publication_date"),
        "version": m.get("version"),
        "creators": [_legacy_person(p) for p in m.get("creators") or []],
        "contributors": [_legacy_person(p, contributor=True) for p in m.get("contributors") or []],
        "communities": [{"id": e["slug"]} for e in (parent.get("communities") or {}).get("entries")
                        or [] if e.get("slug")],
        "related_identifiers": related,
        "references": [r["reference"] for r in m.get("references") or [] if r.get("reference")],
        "notes": notes,
    }
    journal = (hit.get("custom_fields") or {}).get("journal:journal")
    if isinstance(journal, dict):
        meta["journal"] = journal
    files = []
    for key, f in ((hit.get("files") or {}).get("entries") or {}).items():
        key = f.get("key") or key
        files.append({"id": f.get("id"), "key": key, "size": f.get("size"),
                      "checksum": f.get("checksum"),
                      "links": {"self": f"{base}/api/records/{rid}/files/"
                                        f"{quote(key, safe='/')}/content"}})
    owner = (parent.get("access") or {}).get("owned_by") or {}
    return {
        "id": int(rid) if rid.isdigit() else rid,
        "conceptrecid": parent.get("id"),
        "doi": ((hit.get("pids") or {}).get("doi") or {}).get("identifier"),
        "conceptdoi": ((parent.get("pids") or {}).get("doi") or {}).get("identifier") or "",
        "created": hit.get("created"),
        "owners": [{"id": str(owner["user"])}] if owner.get("user") else [],
        "links": {"self_html": (hit.get("links") or {}).get("self_html")},
        "metadata": meta,
        "files": files,
        "_serializer": "inveniordm",
    }


def _person(p: dict[str, Any]) -> dict[str, Any]:
    out = {"name": p.get("name") or ""}
    for k in ("orcid", "affiliation", "type"):
        if p.get(k):
            out[k] = p[k]
    return out


def slim_hit(rec: dict[str, Any]) -> dict[str, Any]:
    """The part of a search hit the census keeps: what :meth:`Candidate.from_record` reads (so a
    keep-list entry built from it is identical to one built from the full hit, bar a description
    beyond ``DESC_MAX``) plus the scoring inputs — depositor account, ORCIDs, affiliations,
    communities, related identifiers, references, notes, journal, subjects."""
    meta = rec.get("metadata") or {}
    keep_meta: dict[str, Any] = {
        "title": meta.get("title") or "",
        "description": (meta.get("description") or "")[:DESC_MAX],
        "keywords": meta.get("keywords") or [],
        "subjects": meta.get("subjects") or [],
        "resource_type": meta.get("resource_type"),
        "license": meta.get("license"),
        "access_right": meta.get("access_right"),
        "publication_date": meta.get("publication_date"),
        "version": meta.get("version"),
        "creators": [_person(c) for c in meta.get("creators") or []],
        "contributors": [_person(c) for c in meta.get("contributors") or []],
        "communities": meta.get("communities") or [],
        "related_identifiers": meta.get("related_identifiers") or [],
        "references": (meta.get("references") or [])[:MAX_REFERENCES],
        "notes": (meta.get("notes") or "")[:NOTES_MAX],
    }
    journal = meta.get("journal")
    if isinstance(journal, dict) and journal.get("title"):
        keep_meta["journal"] = {"title": journal["title"]}
    files = [{"key": f.get("key"), "size": f.get("size"), "checksum": f.get("checksum"),
              "links": {"self": (f.get("links") or {}).get("self")}}
             for f in rec.get("files") or []]
    out = {
        "id": rec.get("id"),
        "conceptrecid": rec.get("conceptrecid"),
        "doi": rec.get("doi"),
        "conceptdoi": rec.get("conceptdoi"),
        "created": rec.get("created"),
        "owners": rec.get("owners") or [],
        "links": {"self_html": (rec.get("links") or {}).get("self_html")},
        "metadata": keep_meta,
        "files": files,
    }
    if rec.get("_serializer"):
        out["_serializer"] = rec["_serializer"]
    return out


def run_census(client: ZenodoClient, out_path: str | Path, *, query: str = CENSUS_QUERY,
               start: date = date(2013, 1, 1), end: date | None = None,
               page_size: int | None = None, max_records: int | None = None,
               fresh: bool = False) -> dict[str, Any]:
    """Enumerate ``query`` exhaustively (recursive ``created`` bisection past the 10k window) and
    append each hit, slimmed, to ``out_path``; resumable at leaf-window granularity.

    The first run fixes the ``created`` range (``end`` defaults to that day) in
    ``<out>.bounds.json``; later runs reuse it (so the bisection — and every finished window —
    is the same) unless ``fresh``. ``page_size`` defaults to 100 with a token and 25 without
    (Zenodo's caps). ``max_records`` stops early (smoke tests). Records the default serializer
    cannot return (:meth:`CensusClient.iter_window`) are logged to ``<out>.poison.jsonl`` — the
    native hit of each converted one, the window and offset of any left unresolved."""
    out = Path(out_path)
    windows_path = Path(str(out) + ".windows.jsonl")
    bounds_path = Path(str(out) + ".bounds.json")
    poison_path = Path(str(out) + ".poison.jsonl")
    if fresh:
        for p in (out, windows_path, bounds_path, poison_path):
            p.unlink(missing_ok=True)
    if bounds_path.is_file():
        b = json.loads(bounds_path.read_text())
        if b.get("query") != query:
            raise ValueError(f"{bounds_path} was made for another query; pass fresh=True")
        start, end = date.fromisoformat(b["start"]), date.fromisoformat(b["end"])
    else:
        end = end or date.today()
        out.parent.mkdir(parents=True, exist_ok=True)
        bounds_path.write_text(json.dumps({"query": query, "start": start.isoformat(),
                                           "end": end.isoformat()}))
    truncate_torn_tail(out)
    truncate_torn_tail(windows_path)
    truncate_torn_tail(poison_path)
    done: set[tuple[str, str, str]] = set()
    if windows_path.is_file():
        done = {(w["query"], w["start"], w["end"]) for w in _records(windows_path)}
    size = page_size or (PAGE_SIZE_TOKEN if client.token else PAGE_SIZE_ANON)
    stats = {"written": 0, "windows_done": 0, "windows_skipped": 0, "poison_converted": 0,
             "poison_unresolved": 0}
    t0 = time.monotonic()

    def should_skip(s: datetime, e: datetime) -> bool:
        if (query, s.isoformat(), e.isoformat()) in done:
            stats["windows_skipped"] += 1
            return True
        return False

    with JsonlWriter(out) as w, JsonlWriter(windows_path) as ww, \
            JsonlWriter(poison_path) as pw:
        def on_poison(event: dict[str, Any]) -> None:
            pw.write(event)
            stats[f"poison_{event['status']}"] += 1

        if isinstance(client, CensusClient):
            client.on_poison = on_poison

        def on_window_done(s: datetime, e: datetime) -> None:
            ww.write({"query": query, "start": s.isoformat(), "end": e.isoformat()})
            stats["windows_done"] += 1
            logger.info("census: window %s..%s done — %d records so far (%.0f min)",
                        s.date(), e.date(), stats["written"], (time.monotonic() - t0) / 60)

        for rec in client.iter_records(query, start=start, end=end, size=size,
                                       should_skip=should_skip, on_window_done=on_window_done):
            w.write(slim_hit(rec))
            stats["written"] += 1
            if max_records and stats["written"] >= max_records:
                break
    summary = {"out": str(out), "query": query, "page_size": size, "start": start.isoformat(),
               "end": end.isoformat(), **stats,
               "minutes": round((time.monotonic() - t0) / 60, 1)}
    logger.info("census: %s", summary)
    return summary


def truncate_torn_tail(path: str | Path) -> int:
    """Cut a torn final line (a job killed mid-append leaves one without its newline) back to the
    last complete line, BEFORE appending: appending after it would glue the next record onto the
    torn one and leave a malformed line mid-file, which the strict shared JSONL reader rejects.
    The torn record is lost, but every writer here re-derives it (an unfinished census window is
    re-paged, an un-cached peek or lookup is redone). Returns the bytes removed."""
    p = Path(path)
    if not p.is_file() or p.stat().st_size == 0:
        return 0
    with p.open("rb+") as fh:
        size = fh.seek(0, 2)
        fh.seek(size - 1)
        if fh.read(1) == b"\n":
            return 0
        pos, chunk = size, 1 << 16
        while pos > 0:
            start = max(0, pos - chunk)
            fh.seek(start)
            buf = fh.read(pos - start)
            i = buf.rfind(b"\n")
            if i >= 0:
                keep = start + i + 1
                fh.truncate(keep)
                return size - keep
            pos = start
        fh.truncate(0)
        return size


def _records(path: str | Path) -> Iterator[dict[str, Any]]:
    """Every parseable census line (a torn line anywhere — possible after a kill + resume — is
    skipped and counted; its record is re-paged by the resumed window anyway)."""
    bad = 0
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                bad += 1
    if bad:
        logger.warning("census %s: skipped %d torn line(s)", path, bad)


# slim_hit() writes "id" then "conceptrecid" first, so the dedup pass can read them cheaply
_ID_PREFIX = re.compile(r'^\{"id": "?(\d+)"?, "conceptrecid": (?:"(\d+)"|null)')


def census_keys(path: str | Path) -> set[str]:
    """Every recid AND conceptrecid in the census (one cheap pass over the line prefixes, with a
    JSON fallback) — what a Zenodo id cited elsewhere must match to be a census record without
    further resolution."""
    keys: set[str] = set()
    with Path(path).open() as fh:
        for line in fh:
            m = _ID_PREFIX.match(line)
            if m:
                keys.add(m.group(1))
                if m.group(2):
                    keys.add(m.group(2))
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            keys.add(str(rec.get("id")))
            if rec.get("conceptrecid"):
                keys.add(str(rec["conceptrecid"]))
    return keys


def iter_census(path: str | Path) -> Iterator[dict[str, Any]]:
    """Deduplicated census records: the newest version (largest recid) of each concept, once —
    its LAST copy when a re-paged window wrote it twice.

    Two streaming passes (the first reads only each line's id prefix into
    ``concept -> (newest recid, line number)``), so a multi-GB census never has to fit in memory."""
    best: dict[str, tuple[int, int]] = {}
    with Path(path).open() as fh:
        for n, line in enumerate(fh):
            m = _ID_PREFIX.match(line)
            if m:
                rid, key = int(m.group(1)), m.group(2) or m.group(1)
            else:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rid = int(rec["id"])
                key = str(rec.get("conceptrecid") or rid)
            prev = best.get(key)
            if prev is None or rid >= prev[0]:
                best[key] = (rid, n)
    wanted = {n for _, n in best.values()}
    with Path(path).open() as fh:
        for n, line in enumerate(fh):
            if n not in wanted:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning("census %s: torn line %d skipped", path, n)
