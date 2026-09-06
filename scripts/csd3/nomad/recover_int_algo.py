#!/usr/bin/env python3
"""Recover the ~1,380 vasprun calcs that hit the pymatgen numeric-ALGO bug.

Diagnosis (2026-09-06): pymatgen 2026.5.4 crashes in ``Vasprun.__init__`` when an upload's
INCAR has a NUMERIC ``ALGO`` (e.g. ``ALGO = 68``) — ``converged_electronic`` does
``self.incar.get("ALGO", "").lower()`` and ``int`` has no ``.lower()``. The files are valid;
only pymatgen mishandles them.

FIX (consistency-first): stay on the SAME pymatgen 2026.5.4 as the existing 7M calcs and apply
a SURGICAL monkeypatch that coerces a non-str ALGO to str before the original property runs —
so the re-parsed data is exactly what pymatgen would have produced (no upstream upgrade, which
could shift run_type tables / energy handling and diverge from the existing data). The parse
runs IN-PROCESS (``parse_timeout_s=0``) so the monkeypatch is in effect (a forkserver child
would import a clean, unpatched pymatgen).

STEP 1 (build the subset manifest) must be run first on a login node:
    python scripts/csd3/nomad/build_reparse_manifest.py --reason vasprun_parse_error \
        --signature "has no attribute 'lower'" \
        --out $NOMAD_HARVEST_DATA/manifests/nomad_int_algo_fetched.jsonl
This script (STEP 2) is launched by recover_int_algo.sh on a compute node.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# repo root on sys.path (see recover_dedup_keep.py)
for _root in Path(__file__).resolve().parents:
    if (_root / "nomad_harvest").is_dir():
        sys.path.insert(0, str(_root))
        break

# --- surgical monkeypatch: coerce a numeric INCAR ALGO to str -------------------------------
import pymatgen.io.vasp.outputs as _pmg  # noqa: E402

_orig_converged_electronic = _pmg.Vasprun.converged_electronic.fget


def _safe_converged_electronic(self):  # type: ignore[no-untyped-def]
    algo = self.incar.get("ALGO", "")
    if not isinstance(algo, str):
        # Incar is a dict subclass; store the string form so the original property's
        # `.lower()` works. INCAR values are strings in the file anyway, so "68" is a
        # faithful representation and does not affect run_type/functional classification.
        self.incar["ALGO"] = str(algo)
    return _orig_converged_electronic(self)


_pmg.Vasprun.converged_electronic = property(_safe_converged_electronic)
print("[recover-int-algo] patched pymatgen", file=sys.stderr)

from zenodo_harvest.parse import parse  # noqa: E402

USER = os.environ["USER"]
NOMAD = Path(os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/nomad"))
FETCHED = NOMAD / "manifests" / "nomad_int_algo_fetched.jsonl"
DS = NOMAD / "dataset"
RAW = NOMAD / "raw"


def main() -> int:
    if not FETCHED.is_file():
        print(f"ERROR: {FETCHED} not found — run build_reparse_manifest.py first "
              f"(reason=vasprun_parse_error, signature=\"has no attribute 'lower'\")", file=sys.stderr)
        return 2
    # retry_rejected=True re-attempts the terminal vasprun_parse_error calcs in this manifest
    # (they are ONLY the ALGO ones — the manifest is scoped to that signature). In-process
    # (parse_timeout_s=0) so the monkeypatch applies; max_primary_bytes guards a stray big one.
    summary = parse(str(FETCHED), dataset_dir=str(DS), raw_dir=str(RAW),
                    rejections_path=str(DS / "rejections.jsonl"),
                    max_primary_bytes=1_600_000_000, parse_timeout_s=0, retry_rejected=True)
    print("[recover-int-algo] parse summary:", summary, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
