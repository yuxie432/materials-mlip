"""Load the scan outputs of one source into flat numpy tables for the report.

* :func:`load_frames` — every shard's ``frames`` rows concatenated (one row per frame) plus the
  summed per-shard aggregates (per-atom |F| histogram, Properties layouts, comment keys, ...).
* :func:`load_calc_structs` — one :data:`STRUCT_DTYPE` row per calc: descriptors of its FIRST
  frame (lowest step over every shard it appears in) and of its LAST frame (highest step), and the
  number of frames seen, with formulas/chemical systems as codes into returned tables. A calc's
  composition is recovered as reduced formula x (natoms / atoms per formula unit).
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

from .common import FORCE_EDGES, array_text
from .scan import FRAME_DTYPE, scan_outputs

STRUCT_DTYPE = np.dtype([
    ("calc", "u8"), ("n_seen", "i4"), ("fstep", "i4"), ("lstep", "i4"), ("natoms", "i4"),
    ("formula", "i4"), ("chemsys", "i4"), ("nel", "i2"),
    ("vol", "f4"), ("rho", "f4"), ("gap0", "f4"), ("gap1", "f4"), ("gap2", "f4"),
    ("dmin", "f4"), ("spg", "i2"), ("rlen0", "f4"), ("rlen1", "f4"), ("rlen2", "f4"),
    ("shash", "u8"),
    ("l_vol", "f4"), ("l_gap0", "f4"), ("l_gap1", "f4"), ("l_gap2", "f4"), ("l_dmin", "f4"),
    ("l_spg", "i2"), ("l_shash", "u8"),
    ("group", "i4"),      # reference datasets: material / parent id code (-1 for the harvest)
])

_FORMULA_RE = re.compile(r"([A-Z][a-z]?)(\d*)")


def formula_counts(formula: str) -> dict[str, int]:
    """``Fe2O3`` -> ``{'Fe': 2, 'O': 3}`` (the reduced canonical formula of :mod:`structure`)."""
    out: dict[str, int] = {}
    for el, n in _FORMULA_RE.findall(formula):
        out[el] = out.get(el, 0) + (int(n) if n else 1)
    return out


def load_frames(scan_dir: str | Path) -> tuple[np.ndarray, dict[str, Any]]:
    parts: list[np.ndarray] = []
    agg: dict[str, Any] = {"shards": 0, "frames": 0, "atoms": 0, "truncated": [], "torn": [],
                           "bad_frames": 0, "fallback_runs": 0, "force_nonfinite_atoms": 0,
                           "force_hist": np.zeros(len(FORCE_EDGES) - 1, dtype=np.int64),
                           "props": Counter(), "info_keys": Counter(), "pbc": Counter(),
                           "scan_cpu_s": 0.0}
    for p in scan_outputs(scan_dir):
        with np.load(p, allow_pickle=False) as z:
            parts.append(z["frames"])
            a = json.loads(array_text(z["agg"]))
        agg["shards"] += 1
        agg["frames"] += a["frames"]
        agg["atoms"] += a["atoms"]
        if a.get("truncated"):
            agg["truncated"].append(a["shard"])
        if a.get("torn_tail"):
            agg["torn"].append(a["shard"])
        for k in ("bad_frames", "fallback_runs", "force_nonfinite_atoms"):
            agg[k] += int(a.get(k, 0))
        agg["force_hist"] += np.asarray(a["force_hist"], dtype=np.int64)
        for k in ("props", "info_keys", "pbc"):
            agg[k].update(a.get(k, {}))
        agg["scan_cpu_s"] += float(a.get("elapsed_s", 0.0))
    frames = np.concatenate(parts) if parts else np.zeros(0, dtype=FRAME_DTYPE)
    return frames, agg


def _descr_fields(d: dict | None) -> tuple:
    if not d:
        return (np.nan,) * 5 + (-1, 0)
    gaps = d.get("gaps") or [np.nan] * 3
    return (d.get("vol", np.nan), gaps[0], gaps[1], gaps[2],
            np.nan if d.get("dmin") is None else d["dmin"],
            -1 if d.get("spg") is None else d["spg"], int(d.get("shash", 0)))


def load_calc_structs(scan_dir: str | Path) -> tuple[np.ndarray, list[str], list[str], list[str]]:
    """Per-calc structure table + the formula, chemsys and group code tables."""
    formulas: dict[str, int] = {}
    systems: dict[str, int] = {}
    groups: dict[str, int] = {}
    rows: list[tuple] = []
    for p in scan_outputs(scan_dir):
        with np.load(p, allow_pickle=False) as z:
            txt = array_text(z["calcs"])
        for line in txt.splitlines():
            if not line:
                continue
            c = json.loads(line)
            f = c.get("first")
            last = c.get("last") or f
            if f is None:
                continue
            fcode = formulas.setdefault(f["formula"], len(formulas))
            scode = systems.setdefault(f["chemsys"], len(systems))
            gaps = f.get("gaps") or [np.nan] * 3
            rlen = f.get("rlen") or [np.nan] * 3
            lv, lg0, lg1, lg2, ld, ls, lh = _descr_fields(last)
            g = groups.setdefault(c["g"], len(groups)) if "g" in c else -1
            rows.append((int(c["k"]), int(c.get("n", 0)), int(f.get("step", -1)),
                         int(last.get("step", -1)), int(f["natoms"]), fcode, scode,
                         int(f.get("nel", 0)), f.get("vol", np.nan), f.get("rho", np.nan),
                         gaps[0], gaps[1], gaps[2],
                         np.nan if f.get("dmin") is None else f["dmin"],
                         -1 if f.get("spg") is None else f["spg"], rlen[0], rlen[1], rlen[2],
                         int(f.get("shash", 0)), lv, lg0, lg1, lg2, ld, ls, lh, g))
    arr = np.array(rows, dtype=STRUCT_DTYPE) if rows else np.zeros(0, dtype=STRUCT_DTYPE)
    arr = _merge_straddling(arr)
    return arr, list(formulas), list(systems), list(groups)


_FIRST_FIELDS = ("fstep", "natoms", "formula", "chemsys", "nel", "vol", "rho", "gap0", "gap1",
                 "gap2", "dmin", "spg", "rlen0", "rlen1", "rlen2", "shash", "group")
_LAST_FIELDS = ("lstep", "l_vol", "l_gap0", "l_gap1", "l_gap2", "l_dmin", "l_spg", "l_shash")


def _merge_straddling(arr: np.ndarray) -> np.ndarray:
    """One row per calc: a calc split across shards has one row per shard; keep the first-frame
    fields of its lowest-step row, the last-frame fields of its highest-step row, and sum n_seen."""
    if len(arr) == 0:
        return arr
    order = np.lexsort((arr["fstep"], arr["calc"]))
    a = arr[order]
    uniq, start = np.unique(a["calc"], return_index=True)
    if len(uniq) == len(a):
        return a
    out = a[start].copy()
    out["n_seen"] = np.add.reduceat(a["n_seen"], start)
    # last-frame fields from the row with the highest lstep within each calc
    order_l = np.lexsort((a["lstep"], a["calc"]))
    b = a[order_l]
    ends = np.append(start[1:], len(b)) - 1
    for f in _LAST_FIELDS:
        out[f] = b[f][ends]
    for f in _FIRST_FIELDS:  # already from the lowest-fstep row (lexsort above)
        out[f] = a[f][start]
    return out
