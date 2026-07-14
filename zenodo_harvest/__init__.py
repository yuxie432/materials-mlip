"""zenodo_harvest — harvest DFT (VASP) datasets from Zenodo for MLIP training.

Pipeline stages (see docs/DESIGN.md); all five run end-to-end via
``python -m zenodo_harvest.cli {discover,triage,fetch,parse}``:

    stage 0  discover  keyword/date-partitioned search -> candidate manifest (JSONL)
    stage 1  triage    score candidates by their file listing (+ optional zip peek)
    stage 2  fetch     download selected archives (checksum-verified) + extract VASP
    stage 3  parse     pymatgen Vasprun/Vaspout / ASE OUTCAR -> per-ionic-step frames
    stage 4  store      sharded extxyz.gz + metadata JSONL store

Stages 0-1 need only ``requests``; stages 2-4 add ``pymatgen`` + ``ase``.
"""

from .client import ZenodoClient
from .models import Candidate, classify_files

__all__ = ["ZenodoClient", "Candidate", "classify_files"]
__version__ = "0.1.0"
