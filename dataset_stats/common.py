"""Shared helpers: the calc join key, fixed histogram bins, atomic writes, summary statistics."""

from __future__ import annotations

import hashlib
import io
import math
import os
from pathlib import Path
from typing import Any

import numpy as np

# eV/A^3 -> GPa (ase.units.GPa = 1/160.21766...)
EV_A3_TO_GPA = 160.21766208
# Per-atom |F| histogram edges (eV/A): an exact-zero bin, 0.05-dex bins from 1e-6 to 1e3, overflow.
FORCE_EDGES = np.concatenate(([0.0], 10.0 ** np.arange(-6.0, 3.0 + 1e-9, 0.05), [np.inf]))
SOURCES = ("zenodo", "nomad", "materials_cloud")


def calc_key(calc_id: str) -> int:
    """64-bit join key of a calc_id (blake2b). The scan and the metadata pass both use it, so a
    frame row joins its calc without carrying the long id string; at ~7.5M calcs the collision
    probability is ~1e-6."""
    return int.from_bytes(hashlib.blake2b(calc_id.encode("utf-8"), digest_size=8).digest(),
                          "little")


def atomic_write_npz(path: str | Path, **arrays: np.ndarray) -> None:
    """``np.savez_compressed`` to a temp file, then ``os.replace`` (a killed job leaves no
    half-written output that a resume would mistake for done)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as fh:
        np.savez_compressed(fh, **arrays)  # type: ignore[arg-type]
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def text_array(text: str) -> np.ndarray:
    """UTF-8 text as a uint8 array (np.savez stores str arrays as UTF-32: 4x the size)."""
    return np.frombuffer(text.encode("utf-8"), dtype=np.uint8)


def array_text(arr: np.ndarray) -> str:
    return arr.tobytes().decode("utf-8")


def jsonl_text(rows: list[dict]) -> str:
    import json
    buf = io.StringIO()
    for r in rows:
        buf.write(json.dumps(r, separators=(",", ":")))
        buf.write("\n")
    return buf.getvalue()


def quantiles(x: np.ndarray, qs: tuple[float, ...] = (0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95,
                                                      0.99, 1.0),
              weights: np.ndarray | None = None) -> dict[str, float | None]:
    """Quantiles of the finite values of ``x`` (optionally weighted), keyed ``p50`` etc."""
    x = np.asarray(x, dtype=float)
    ok = np.isfinite(x)
    if weights is not None:
        w = np.asarray(weights, dtype=float)
        ok &= np.isfinite(w) & (w > 0)
    if not ok.any():
        return {_qkey(q): None for q in qs}
    xv = x[ok]
    if weights is None:
        vals = np.quantile(xv, qs)
    else:
        wv = w[ok]
        order = np.argsort(xv)
        xs, cw = xv[order], np.cumsum(wv[order])
        cw = cw / cw[-1]
        vals = np.array([xs[min(int(np.searchsorted(cw, q)), len(xs) - 1)] for q in qs])
    return {_qkey(q): float(v) for q, v in zip(qs, vals)}


def _qkey(q: float) -> str:
    if q == 0.0:
        return "min"
    if q == 1.0:
        return "max"
    return f"p{q * 100:g}"


def summary(x: np.ndarray, weights: np.ndarray | None = None) -> dict[str, Any]:
    """count / mean / quantiles of the finite values (+ how many were non-finite)."""
    x = np.asarray(x, dtype=float)
    ok = np.isfinite(x)
    out: dict[str, Any] = {"n": int(ok.sum()), "n_nonfinite": int((~ok).sum())}
    if ok.any():
        if weights is None:
            out["mean"] = float(x[ok].mean())
        else:
            w = np.asarray(weights, dtype=float)[ok]
            out["mean"] = float((x[ok] * w).sum() / max(w.sum(), 1e-300))
    out.update(quantiles(x, weights=weights))
    return out


def hist(x: np.ndarray, edges: np.ndarray, weights: np.ndarray | None = None) -> dict[str, Any]:
    """Histogram of the finite values with explicit edges (JSON-ready)."""
    x = np.asarray(x, dtype=float)
    ok = np.isfinite(x)
    w = None if weights is None else np.asarray(weights, dtype=float)[ok]
    counts, _ = np.histogram(x[ok], bins=edges, weights=w)
    return {"edges": [float(e) for e in edges], "counts": counts.tolist(),
            "below": int((x[ok] < edges[0]).sum()), "above": int((x[ok] > edges[-1]).sum())}


def hist_quantile(edges: np.ndarray, counts: np.ndarray, q: float) -> float | None:
    """Approximate quantile from a histogram (log-linear interpolation inside the bin)."""
    counts = np.asarray(counts, dtype=float)
    tot = counts.sum()
    if tot <= 0:
        return None
    c = np.cumsum(counts) / tot
    i = int(np.searchsorted(c, q))
    i = min(i, len(counts) - 1)
    lo, hi = float(edges[i]), float(edges[i + 1])
    prev = c[i - 1] if i > 0 else 0.0
    frac = (q - prev) / max(c[i] - prev, 1e-300)
    if not math.isfinite(hi):
        return lo
    if lo > 0:
        return float(math.exp(math.log(lo) + frac * (math.log(hi) - math.log(lo))))
    return lo + frac * (hi - lo)


def concentration(sizes: np.ndarray) -> dict[str, Any]:
    """How concentrated a total is across groups (e.g. frames across deposits): top-k shares,
    Gini, and the effective number of groups 1/sum(p^2) (inverse Herfindahl) and exp(entropy)."""
    s = np.sort(np.asarray(sizes, dtype=float))[::-1]
    s = s[s > 0]
    if not len(s):
        return {"groups": 0}
    p = s / s.sum()
    n = len(s)
    cum = np.cumsum(np.sort(s))
    gini = float((n + 1 - 2 * (cum / cum[-1]).sum()) / n)
    out = {"groups": int(n), "total": float(s.sum()), "gini": round(gini, 4),
           "effective_n_inverse_hhi": round(float(1.0 / (p * p).sum()), 2),
           "effective_n_entropy": round(float(np.exp(-(p * np.log(p)).sum())), 2)}
    for k in (1, 3, 10, 100):
        out[f"top{k}_share"] = round(float(p[:k].sum()), 4)
    return out


def counter_top(counter: dict[str, Any], k: int = 30) -> list[tuple[str, Any]]:
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:k]
