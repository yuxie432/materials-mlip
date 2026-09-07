#!/usr/bin/env python3
"""Recover the ~1,380 vasprun calcs that hit the pymatgen numeric-ALGO bug.

Diagnosis (2026-09-06): pymatgen 2026.5.4 crashes in ``Vasprun.__init__`` when an upload's
INCAR has a NUMERIC ``ALGO`` (e.g. ``ALGO = 68``) — ``converged_electronic`` does
``self.incar.get("ALGO", "").lower()`` and ``int`` has no ``.lower()``. The files are valid;
only pymatgen mishandles them.

FIX (consistency-first): stay on the SAME pymatgen 2026.5.4 as the existing 7M calcs and apply
a surgical monkeypatch that coerces a non-str ALGO to str before the original property runs, so
the re-parsed data is exactly what pymatgen would have produced (no upstream upgrade, which
could shift run_type tables / energy handling and diverge from the existing data).

NB the coercion writes into ``self.incar.data`` (the UserDict store), NOT ``self.incar["ALGO"]``:
``Incar.__setitem__`` runs ``proc_val`` which re-coerces ``"68"`` straight back to ``int 68``, so
a plain assignment is a silent no-op (verified). The parse runs IN-PROCESS (``parse_timeout_s=0``)
so the monkeypatch is in effect — a forkserver child would import a clean, unpatched pymatgen.

This is STEP 2, launched by recover_int_algo.sh on a compute node (which builds the subset
manifest first). It reads ``$NOMAD_HARVEST_DATA/manifests/nomad_int_algo_fetched.jsonl``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# repo root on sys.path, so `python scripts/csd3/nomad/recover_int_algo.py` can import the
# packages (running a script by path puts the SCRIPT dir on sys.path, not the repo root).
for _root in Path(__file__).resolve().parents:
    if (_root / "nomad_harvest").is_dir():
        sys.path.insert(0, str(_root))
        break

# --- surgical monkeypatch: coerce a numeric INCAR ALGO to str ------------------------------
import pymatgen.io.vasp.outputs as _pmg  # noqa: E402

_orig_converged_electronic = _pmg.Vasprun.converged_electronic.fget


def _safe_converged_electronic(self):  # type: ignore[no-untyped-def]
    algo = self.incar.get("ALGO", "")
    if not isinstance(algo, str):
        # Incar is a UserDict; its __setitem__ -> proc_val would re-coerce "68" back to int 68,
        # so write the raw string into the underlying store to make the original .lower() work.
        try:
            self.incar.data["ALGO"] = str(algo)
        except Exception:  # noqa: BLE001 - ultra-defensive; a miss just re-rejects the calc
            pass
    return _orig_converged_electronic(self)


_pmg.Vasprun.converged_electronic = property(_safe_converged_electronic)
print("[recover-int-algo] patched pymatgen converged_electronic (numeric-ALGO guard)", file=sys.stderr)

from zenodo_harvest.parse import parse  # noqa: E402

USER = os.environ["USER"]
NOMAD = Path(os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/nomad"))
FETCHED = NOMAD / "manifests" / "nomad_int_algo_fetched.jsonl"
DS = NOMAD / "dataset"
RAW = NOMAD / "raw"


def main() -> int:
    if not FETCHED.is_file() or FETCHED.stat().st_size == 0:
        print(f"ERROR: {FETCHED} missing/empty — recover_int_algo.sh builds it before this step",
              file=sys.stderr)
        return 2
    # retry_rejected=True re-attempts the terminal vasprun_parse_error calcs in this manifest
    # (scoped to the ALGO signature). In-process (parse_timeout_s=0) so the monkeypatch applies;
    # max_primary_bytes guards a stray big one.
    summary = parse(str(FETCHED), dataset_dir=str(DS), raw_dir=str(RAW),
                    rejections_path=str(DS / "rejections.jsonl"),
                    max_primary_bytes=1_600_000_000, parse_timeout_s=0, retry_rejected=True)
    print("[recover-int-algo] parse summary:", summary, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
