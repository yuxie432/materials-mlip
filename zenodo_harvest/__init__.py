"""zenodo_harvest — discover and triage DFT (VASP) datasets on Zenodo.

Pipeline stages (see docs/DESIGN.md):

    stage 0  discover  keyword/date-partitioned search -> candidate manifest (JSONL)
    stage 1  triage    score candidates by their file listing (+ optional zip peek)
    stage 2  fetch      download selected archives (checksum-verified)      [next step]
    stage 3  parse      pymatgen Vasprun/Outcar -> per-ionic-step frames     [next step]
    stage 4  store      sharded extxyz.gz + metadata store                   [next step]

Only stages 0 and 1 are implemented here; they need nothing beyond `requests`.
"""

from .client import ZenodoClient
from .models import Candidate, classify_files

__all__ = ["ZenodoClient", "Candidate", "classify_files"]
__version__ = "0.1.0"
