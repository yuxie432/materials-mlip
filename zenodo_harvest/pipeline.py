"""Overlapped harvest orchestration: run ``fetch`` (network-bound) for part *i+1* at
the same time as ``parse``+``purge-raw`` (CPU/disk-bound) for part *i*, so the network
is not idle during parsing and vice-versa.

The core :func:`run_pipeline` is deliberately I/O-agnostic — it takes plain callables,
so it is unit-testable without the network or pymatgen. The CLI (`cli.py pipeline`)
supplies the real callables: ``fetch_fn`` = stage-2 fetch of one part into that part's
own ``fetched.part-N.jsonl`` (never a shared file, so a background parse always reads a
*complete* manifest), ``process_fn`` = ``parse`` that part into the shared dataset dir
(parses serialize on the dataset's ``.parse.lock`` — ``after_workers=1`` keeps them
one-at-a-time) then ``purge-raw`` that part's staged archives.

Disk safety: while ``process_fn(i)`` runs, part *i*'s staged files are still on disk
(until its purge) *and* ``fetch_fn(i+1)`` is staging part *i+1* — so at most **two**
parts' staging coexist. That needs no extra allowance in the budget: fetch's
``--max-disk-bytes`` valve measures the WHOLE raw staging tree (it baselines on
``_dir_bytes(raw_dir)``, which already includes the not-yet-purged part *i*), so it bounds
the total across both parts rather than per part. See ``fetch``'s docstring for how to
size it. If a part's fetch nonetheless hits that valve mid-batch
it reports itself incomplete (``fetch_fn`` returns False) and the orchestrator drains
the outstanding parse+purge to reclaim staging and *resumes the same part* — a partly
fetched batch is never carried on and silently forgotten (see :func:`run_pipeline`).
"""

from __future__ import annotations

import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Callable

logger = logging.getLogger(__name__)


# Backstop on the fetch->parse->purge->resume pacing loop for ONE part. It should never
# fire: stage-2 fetch always admits at least one record per call (its disk budget admits
# a single over-budget record alone rather than deadlocking), so every pass makes
# progress and the loop terminates on its own. The cap only guards against a fetch_fn
# that reports "incomplete" while fetching nothing at all — an unattended job must not
# spin forever on that.
DEFAULT_MAX_FETCH_PASSES = 200


def run_pipeline(
    parts: list[Any],
    fetch_fn: Callable[[Any], Any],
    process_fn: Callable[[Any], Any],
    after_workers: int = 1,
    max_fetch_passes: int = DEFAULT_MAX_FETCH_PASSES,
) -> tuple[list[Any], list[tuple[Any, Exception]]]:
    """Fetch each part in the foreground; process it in the background, overlapping the
    processing of part *i* with the fetch of part *i+1*.

    ``after_workers`` bounds how many ``process_fn`` calls may run at once (default 1:
    one background parse+purge overlapping the current fetch — the safe choice when all
    parts parse into one dataset dir guarded by a single lock). Returns
    ``(done_parts, errors)`` where ``errors`` is ``[(part, exception), …]`` for any
    ``process_fn`` that raised (a fetch that raises propagates immediately — it is the
    foreground step and a failed download batch should stop the run, not be swallowed).

    **Incomplete fetches.** ``fetch_fn`` may return ``False`` to mean "this part is only
    PARTLY fetched" — which is exactly what stage-2 fetch does when it stops on its
    ``--max-disk-bytes`` valve. The part is then *not* handed on and forgotten (that
    would silently drop records from the harvest). Instead we reclaim staging and resume
    the same part: first by draining any outstanding ``process_fn`` jobs (older parts,
    already fully fetched), and failing that by processing what *this* part has staged so
    far — parse+purge are idempotent, so a part may be processed in several instalments.
    That makes the loop the same "fetch → parse → purge → resume" pacing the disk valve
    is designed for, and it works even for the very first part, where nothing older is
    outstanding. ``max_fetch_passes`` bounds it: needing more cycles than that means the
    budget cannot hold a meaningful fraction of one batch, which is reported as an error
    for that part and stops the run rather than under-fetching every later part. Any
    other return value — including ``None`` — means "complete".
    """
    done: list[Any] = []
    errors: list[tuple[Any, Exception]] = []
    inflight: dict[Any, Any] = {}

    def _collect(fut: Any) -> None:
        part = inflight.pop(fut)
        try:
            fut.result()
            if part not in done:  # a part may be processed more than once (see below)
                done.append(part)
        except Exception as exc:  # a background parse/purge failed; record, keep going
            logger.exception("pipeline processing failed for part %s", part)
            errors.append((part, exc))

    def _drain_all() -> None:
        for f in wait(list(inflight))[0]:
            _collect(f)

    def _fetch_fully(part: Any, pool: ThreadPoolExecutor) -> bool:
        """Fetch ``part`` to completion, reclaiming staging as needed. False if it could
        not be completed (the caller then stops and reports)."""
        for _pass in range(max_fetch_passes):
            if fetch_fn(part) is not False:
                return True
            # Partly fetched: the disk budget is full, and only parse+purge frees it.
            if inflight:
                # Prefer draining OLDER parts — their staging is already fully fetched.
                logger.info("part %s partly fetched (disk budget); draining outstanding "
                            "parse+purge to reclaim staging, then resuming it", part)
                _drain_all()
            else:
                # Nothing older outstanding (e.g. the very first batch, or we already
                # drained): parse+purge what THIS part has staged so far, then carry on
                # fetching the rest of it. Parse and purge are both idempotent, so
                # processing a part in several instalments is safe.
                logger.info("part %s partly fetched (disk budget); parsing+purging its "
                            "staged records to reclaim staging, then resuming it", part)
                inflight[pool.submit(process_fn, part)] = part
                _drain_all()
        errors.append((part, RuntimeError(
            f"part still incomplete after {max_fetch_passes} fetch+parse+purge cycles — "
            f"the disk budget is far too small for one batch; raise --max-disk-bytes or "
            f"raise --parts so each batch is smaller")))
        return False

    with ThreadPoolExecutor(max_workers=after_workers) as pool:
        for part in parts:
            if not _fetch_fully(part, pool):            # foreground: download this part
                break
            inflight[pool.submit(process_fn, part)] = part  # background: parse+purge it
            # Keep at most `after_workers` processing jobs outstanding so staging from
            # already-processed parts is reclaimed before we run too far ahead.
            if len(inflight) > after_workers:
                finished, _ = wait(list(inflight), return_when=FIRST_COMPLETED)
                for f in finished:
                    _collect(f)
        _drain_all()                                    # drain the tail
    return done, errors
