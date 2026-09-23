"""Polite, retrying client for the Materials Cloud Archive REST API (InvenioRDM, reads only).

API facts baked in (live-verified 2026-09-23 — see docs/MATERIALS_CLOUD_HARVEST.md §2):

* Base ``https://archive.materialscloud.org``; **anonymous** reads — every record is public, so
  no token is needed (and none is ever sent: see :meth:`MaterialsCloudClient.new_session`).
* Search ``GET /api/records?q=&size=&page=&sort=``. Each hit carries the full metadata AND the
  file listing INLINE — ``files.entries`` is a **dict keyed by filename** (``key``/``size``/
  ``checksum="md5:…"``/``ext``/``mimetype``) with **no download link**. ``size`` up to 1000 is
  accepted but big pages transfer slowly, so we page at 100. The default search returns only the
  **latest version** of each record (1,241 records; ``allversions=true`` → 1,535). The Invenio
  10k result window is irrelevant at this size and is guarded, not bisected.
* File bytes: ``GET /api/records/{id}/files/{key}/content`` answers **302 → a presigned CSCS S3
  (``rgw.cscs.ch``) URL that expires ~60 s after issue**; the S3 side honours HTTP Range (206,
  incl. a suffix range longer than the file) and serves exactly the md5 the listing declares.
  ``requests`` follows the redirect and keeps the ``Range`` header, and every request mints a
  fresh presigned URL, so resumable downloads / Range peeks never hit an expired signature.
* Rate limit ``X-RateLimit-Limit: 500`` per 60 s window (``Retry-After: 60`` on 429) — generous;
  we still self-throttle and back off on 429/5xx/network errors.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

BASE = "https://archive.materialscloud.org"
USER_AGENT = "materials-mlip-mc/0.1 (MLIP training-data harvest)"
# Invenio's search window: page*size beyond this is refused. MC holds ~1.2k records, so a full
# listing never approaches it; if it ever does, iter_records raises instead of silently truncating.
MAX_SEARCH_WINDOW = 10_000
DEFAULT_PAGE_SIZE = 100
_RETRY_STATUS = {500, 502, 503, 504}
_MAX_429 = 30          # 429s are waited out (not counted as errors), but never forever


def content_url(recid: str, key: str, base: str = BASE) -> str:
    """The download URL of one file of a record (the 302-to-presigned-S3 endpoint).

    ``key`` is percent-encoded completely (``safe=""``): ~13% of MC file keys contain spaces,
    unicode or other reserved characters (e.g. ``Figs. 1c-e, Figs. 2a–e.zip``)."""
    return f"{base.rstrip('/')}/api/records/{quote(str(recid), safe='')}/files/{quote(key, safe='')}/content"


def new_session() -> requests.Session:
    """An ANONYMOUS session with the harvest's User-Agent — for fetch/triage/probes.

    Deliberately carries no ``Authorization`` header: the shared Zenodo fetch would otherwise
    fall back to ``$ZENODO_TOKEN`` and send it to Materials Cloud (the redirect to S3 would strip
    it, but the first hop would already have leaked it)."""
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _retry_after(resp: requests.Response, default: float = 60.0) -> float:
    """Seconds to wait on a 429: ``Retry-After`` (delta seconds), else the time until
    ``X-RateLimit-Reset`` (a Unix timestamp), else ``default``."""
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except ValueError:
            pass
    reset = resp.headers.get("X-RateLimit-Reset")
    if reset:
        try:
            return max(1.0, float(reset) - time.time())
        except ValueError:
            pass
    return default


class MaterialsCloudClient:
    """Wrapper over the Materials Cloud Archive records API.

    ``min_interval`` self-throttles between API calls (500 req/60 s allows ~0.12 s; we stay
    well under). Every call retries 429 (waiting ``Retry-After``/``X-RateLimit-Reset``), 5xx and
    transient network errors with exponential backoff, then raises.
    """

    def __init__(self, base: str = BASE, min_interval: float = 0.25, timeout: float = 300,
                 max_retries: int = 6, session: requests.Session | None = None) -> None:
        self.base = base.rstrip("/")
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or new_session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        self._last = 0.0

    # -- low level -----------------------------------------------------------

    def _throttle(self) -> None:
        wait = self.min_interval - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base}{path}"
        attempt = n429 = 0
        while True:
            self._throttle()
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                self._last = time.monotonic()
                if attempt >= self.max_retries:
                    raise
                wait = min(2 ** (attempt + 1), 60)
                attempt += 1
                logger.warning("MC %s network error %s; retry %d/%d in %ss", path,
                               type(exc).__name__, attempt, self.max_retries, wait)
                time.sleep(wait)
                continue
            self._last = time.monotonic()
            if r.status_code == 429 and n429 < _MAX_429:
                n429 += 1
                wait = _retry_after(r) + 1
                logger.warning("MC rate limited; sleeping %.0fs", wait)
                time.sleep(wait)
                continue
            if r.status_code in _RETRY_STATUS and attempt < self.max_retries:
                wait = min(2 ** (attempt + 1), 60)
                attempt += 1
                logger.warning("MC %s -> HTTP %d; retry %d/%d in %ss", path, r.status_code,
                               attempt, self.max_retries, wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]

    # -- records ---------------------------------------------------------------

    def search_page(self, q: str | None = None, page: int = 1, size: int = DEFAULT_PAGE_SIZE,
                    sort: str = "oldest", all_versions: bool = False) -> dict[str, Any]:
        params: dict[str, Any] = {"size": size, "page": page, "sort": sort}
        if q:
            params["q"] = q
        if all_versions:
            params["allversions"] = "true"
        return self._get_json("/api/records", params)

    def count(self, q: str | None = None, all_versions: bool = False) -> int:
        return int(self.search_page(q, page=1, size=1, all_versions=all_versions)["hits"]["total"])

    def iter_records(self, q: str | None = None, page_size: int = DEFAULT_PAGE_SIZE,
                     sort: str = "oldest", all_versions: bool = False,
                     max_records: int | None = None) -> Iterator[dict[str, Any]]:
        """Yield every record matching ``q`` (all records when ``q`` is None), each once.

        ``sort="oldest"`` (creation order) keeps the page walk stable: a record published while
        the scan runs sorts LAST, so it cannot shift an earlier page and cause a miss; a record
        seen twice across a page boundary is de-duplicated by id here.
        """
        seen: set[str] = set()
        page = 1
        while True:
            d = self.search_page(q, page=page, size=page_size, sort=sort, all_versions=all_versions)
            total = int(d["hits"]["total"])
            if total > MAX_SEARCH_WINDOW:
                raise RuntimeError(
                    f"{total} records exceed the {MAX_SEARCH_WINDOW}-result search window; a full "
                    "listing now needs created-date bisection (see zenodo_harvest.client)")
            hits = d["hits"]["hits"]
            for rec in hits:
                rid = str(rec.get("id"))
                if rid in seen:
                    continue
                seen.add(rid)
                yield rec
                if max_records is not None and len(seen) >= max_records:
                    return
            if not hits or page * page_size >= total:
                return
            page += 1

    def get_record(self, recid: str) -> dict[str, Any]:
        return self._get_json(f"/api/records/{quote(str(recid), safe='')}")

    def list_files(self, recid: str) -> list[dict[str, Any]]:
        """The record's file entries from ``/api/records/{id}/files`` — used only when the
        search hit's inline ``files.entries`` is incomplete (never observed, but cheap to guard)."""
        d = self._get_json(f"/api/records/{quote(str(recid), safe='')}/files")
        ents = d.get("entries") or []
        return list(ents.values()) if isinstance(ents, dict) else list(ents)

    def content_url(self, recid: str, key: str) -> str:
        return content_url(recid, key, self.base)
