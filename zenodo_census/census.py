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
the same rule as discovery).
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator

from zenodo_harvest.client import ZenodoClient
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

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last_start)
        if wait > 0:
            time.sleep(wait)
        self._last_start = time.monotonic()


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
    return {
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


def run_census(client: ZenodoClient, out_path: str | Path, *, query: str = CENSUS_QUERY,
               start: date = date(2013, 1, 1), end: date | None = None,
               page_size: int | None = None, max_records: int | None = None,
               fresh: bool = False) -> dict[str, Any]:
    """Enumerate ``query`` exhaustively (recursive ``created`` bisection past the 10k window) and
    append each hit, slimmed, to ``out_path``; resumable at leaf-window granularity.

    The first run fixes the ``created`` range (``end`` defaults to that day) in
    ``<out>.bounds.json``; later runs reuse it (so the bisection — and every finished window —
    is the same) unless ``fresh``. ``page_size`` defaults to 100 with a token and 25 without
    (Zenodo's caps). ``max_records`` stops early (smoke tests)."""
    out = Path(out_path)
    windows_path = Path(str(out) + ".windows.jsonl")
    bounds_path = Path(str(out) + ".bounds.json")
    if fresh:
        for p in (out, windows_path, bounds_path):
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
    done: set[tuple[str, str, str]] = set()
    if windows_path.is_file():
        done = {(w["query"], w["start"], w["end"]) for w in _records(windows_path)}
    size = page_size or (PAGE_SIZE_TOKEN if client.token else PAGE_SIZE_ANON)
    stats = {"written": 0, "windows_done": 0, "windows_skipped": 0}
    t0 = time.monotonic()

    def should_skip(s: datetime, e: datetime) -> bool:
        if (query, s.isoformat(), e.isoformat()) in done:
            stats["windows_skipped"] += 1
            return True
        return False

    with JsonlWriter(out) as w, JsonlWriter(windows_path) as ww:
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
