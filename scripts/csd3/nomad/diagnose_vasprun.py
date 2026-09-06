#!/usr/bin/env python3
"""Diagnose the ``vasprun_parse_error`` ``'int' object has no attribute 'lower'`` failures.

This is the 1,779-calc subset that shares one signature and smells like a pymatgen parser
limitation (a value expected to be a string arrives as an int) rather than file truncation
(the 2,093 ``IndexError``s). Recovery is only worth it if a code change actually parses them,
so this prints (a) the exact pymatgen/ase/numpy versions and (b) the FULL traceback for a few
such files — the traceback line is what tells us whether a newer pymatgen or a small local
shim fixes it. The staged files are still on disk (parse failures never purge).

    python scripts/csd3/nomad/diagnose_vasprun.py

Then, to test a fix: make a scratch venv, `pip install -U pymatgen`, and re-run Vasprun on the
same file path this prints. Send the version line + traceback for advice.
"""
from __future__ import annotations

import glob
import json
import os
import sys
import traceback
from importlib.metadata import PackageNotFoundError, version

# pymatgen is a namespace package with NO module-level __version__ — use importlib.metadata.
for pkg in ("pymatgen", "ase", "numpy", "lxml"):
    try:
        print(f"{pkg:>10}: {version(pkg)}")
    except PackageNotFoundError:
        print(f"{pkg:>10}: (not installed)")

USER = os.environ["USER"]
NOMAD = os.environ.get("NOMAD_HARVEST_DATA", f"/rds/user/{USER}/hpc-work/nomad")
REJ = f"{NOMAD}/dataset/rejections.jsonl"
SIG = "has no attribute 'lower'"
N = int(sys.argv[1]) if len(sys.argv) > 1 else 3

cids = []
for line in open(REJ):
    line = line.strip()
    if not line:
        continue
    r = json.loads(line)
    if r.get("reason") == "vasprun_parse_error" and SIG in str(r.get("detail", "")):
        cids.append(r["id"])
print(f"\n{len(cids)} calcs with signature {SIG!r}; inspecting up to {N}:\n")

from pymatgen.io.vasp import Vasprun  # noqa: E402 - after version print, so a missing dep is obvious

for cid in cids[:N]:
    eid = str(cid).split(":")[1]
    files = glob.glob(f"{NOMAD}/raw/{eid}/extracted/calc/vasprun.xml*")
    print("=" * 72)
    print("calc:", cid)
    print("file:", files[0] if files else "MISSING (purged or never staged?)")
    if not files:
        continue
    try:
        Vasprun(files[0])
        print("  -> parsed OK now (the installed pymatgen already fixes it!)")
    except Exception:
        traceback.print_exc()
