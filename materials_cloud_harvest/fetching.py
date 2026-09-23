"""Stage 2 glue: the SHARED ``zenodo_harvest.fetch`` + in-run retries of transient failures.

The shared fetch deliberately gives up on a file at the first mid-transfer network error (keeping
its ``.part`` and logging a transient, non-terminal rejection) — the *next run* resumes it over
HTTP Range. That contract suited the Zenodo campaign, which was re-run many times, but an MC
harvest is expected to finish in ONE job that exits 0, and a pipeline that exits cleanly never
resubmits: a unit whose multi-GB tarball dropped once would silently stay unfetched. (Observed live
on the flaky WSL link: ``ChunkedEncodingError`` every few MB.)

:func:`fetch_with_retries` therefore re-runs the shared fetch over the same keep-list while any of
its units is still *pending* — neither in the fetched manifest nor terminally rejected (so exactly
the transient failures) — up to ``retries`` extra passes. Each pass costs only the pending units
(the shared fetch skips done/terminal recids), and each resumes from the kept ``.part`` (a fresh
presigned S3 URL per request), so progress is monotone. A pass stopped by the disk valve is handed
straight back to the pipeline's reclaim-and-resume loop instead.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

from zenodo_harvest.fetch import _done_recids, _terminal_reject_recids
from zenodo_harvest.manifest import read_jsonl

logger = logging.getLogger(__name__)

TRANSIENT_RETRIES = 4


# A unit refused because it does not fit the WHOLE staging budget (nothing, or only part, could be
# staged) is non-terminal by design — raising the budget collects it — but retrying it under the
# SAME budget fails identically. So it is not "pending" (no retry pass, no resubmit churn); it is
# reported separately so the operator raises --max-disk-bytes/--max-disk-files instead.
_NEEDS_BIGGER_BUDGET = "record_exceeds_disk_budget"


def _last_record_reasons(rejections: str | Path) -> dict[str, str]:
    """The LAST record-level fetch rejection reason per recid (per-file ids carry ``:``)."""
    last: dict[str, str] = {}
    p = Path(rejections)
    if p.is_file():
        for r in read_jsonl(p):
            rid = r.get("id")
            if (str(r.get("stage", "")).endswith("fetch") and isinstance(rid, str)
                    and ":" not in rid):
                last[rid] = str(r.get("reason"))
    return last


def units_needing_bigger_budget(in_path: str | Path, out_path: str | Path,
                                rejections: str | Path) -> set[str]:
    """Keep-list recids whose last verdict is ``record_exceeds_disk_budget`` — either nothing
    could be staged (not in ``out_path``) or the unit was staged TRUNCATED (in ``out_path``, its
    partial calcs parse; the rest needs a bigger budget AND the recid removed from the fetched
    manifest). ``out_path`` is accepted for symmetry; both cases are reported."""
    keep = {str(r["recid"]) for r in read_jsonl(in_path) if r.get("recid")}
    last = _last_record_reasons(rejections)
    return {u for u in keep if last.get(u) == _NEEDS_BIGGER_BUDGET}


def pending_units(in_path: str | Path, out_path: str | Path, rejections: str | Path) -> set[str]:
    """Keep-list recids neither fetched (in ``out_path``), terminally rejected, nor waiting only
    for a bigger staging budget — i.e. exactly the transient failures (and never-attempted units)."""
    keep = {str(r["recid"]) for r in read_jsonl(in_path) if r.get("recid")}
    return (keep - _done_recids(Path(out_path)) - _terminal_reject_recids(Path(rejections))
            - units_needing_bigger_budget(in_path, out_path, rejections))


def fetch_with_retries(fetch_call: Callable[[], dict[str, Any]], in_path: str | Path,
                       out_path: str | Path, rejections: str | Path,
                       retries: int = TRANSIENT_RETRIES,
                       sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Run ``fetch_call`` (one shared-fetch pass), then retry while units stay pending.

    Returns the LAST pass's summary, plus ``fetch_passes`` and ``pending_after_retries`` (the
    recids still pending when it gave up — non-empty only if failures persisted)."""
    summary = fetch_call()
    passes = 1
    pending: set[str] = set()
    for i in range(retries):
        if summary.get("stopped_disk_budget"):
            break                         # the pipeline reclaims staging and resumes the part
        pending = pending_units(in_path, out_path, rejections)
        if not pending:
            break
        wait = min(60.0, 10.0 * (i + 1))
        logger.warning("%d fetch unit(s) still pending after a transient failure (%s…); "
                       "retry pass %d/%d in %.0fs", len(pending), sorted(pending)[:3], i + 1,
                       retries, wait)
        sleep(wait)
        summary = fetch_call()
        passes += 1
    else:
        pending = (set() if summary.get("stopped_disk_budget")
                   else pending_units(in_path, out_path, rejections))
    summary["fetch_passes"] = passes
    summary["pending_after_retries"] = sorted(pending) if not summary.get("stopped_disk_budget") else []
    big = sorted(units_needing_bigger_budget(in_path, out_path, rejections))
    if big:
        logger.warning("%d fetch unit(s) do not fit the WHOLE staging budget (%s…): raise "
                       "--max-disk-bytes/--max-disk-files (and drop any truncated one from its "
                       "fetched manifest) to collect them in full", len(big), big[:3])
    summary["units_exceeding_disk_budget"] = big
    return summary
