"""Live smoke of the census deep peeks (zenodo_census/deeppeek.py) on four real Zenodo files — a few
MB of Range reads, ~1 min on a CSD3 login node. Run from the repo root (a FILE, not stdin: the 7z
decoder's child process re-imports its parent script):

    python scripts/csd3/census/deep_peek_smoke.py

Expected (checked 2026-09-29): the 2 GB YF3 tar walks to its end block (16 members, no VASP), the
49 GB PETase 7z lists from its end header (48 members, no VASP), both small zips of archives are
looked inside (status ok). Exits 1 if any peek fails."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from zenodo_census import deeppeek as dp  # noqa: E402

API = "https://zenodo.org/api/records"
CASES = [
    ("tar", f"{API}/7788977/files/010x1HF_a_nr6.tar/content", 2062776832, "010x1HF_a_nr6.tar"),
    ("7z", f"{API}/6611146/files/acylation.7z/content", 49107332913, "acylation.7z"),
    ("zip", f"{API}/18316581/files/DFT-calculations.zip/content", 1032366, None),
    ("zip", f"{API}/17899015/files/zenodo-data.zip/content", 250489, None),
]


def main() -> int:
    s = requests.Session()
    s.headers["User-Agent"] = "zenodo-census/0.1 (materials-mlip research)"
    bad = 0
    for kind, url, size, name in CASES:
        t = time.monotonic()
        if kind == "tar":
            ev = dp.tar_walk(s, url, size)
        elif kind == "7z":
            ev = dp.sevenzip_peek(s, url, size, name=name)
        else:
            ev = dp.nested_zip_peek(s, url, size)
        keys = ("status", "n_members", "n_primary", "primary_sample", "n_nested",
                "n_nested_peeked", "complete", "requests", "bytes_read")
        print(f"{kind:4s} {url.split('/')[-4]:>9} {time.monotonic() - t:5.1f}s",
              {k: ev.get(k) for k in keys if k in ev}, flush=True)
        bad += ev.get("status") != "ok"
    print("deep-peek smoke:", "OK" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
