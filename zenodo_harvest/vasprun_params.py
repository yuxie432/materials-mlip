"""Read a VASP ``vasprun.xml`` **header** into the resolved ``parameters`` block.

The original Zenodo harvest parsed ``vasprun.xml`` *before* :func:`parse._calc_parameters`
began storing VASP's RESOLVED/effective ``parameters`` (the analog of the OUTCAR header's
effective block), so the 83k ``pymatgen.Vasprun`` calcs carry only the user ``incar`` — a tag
left at VASP's default is absent, and integer tags with no stable default (``ISIF``) or flags
VASP omits from the INCAR echo cannot be filled by :mod:`param_resolver` beyond a guess. This
module recovers that block by reading **only the ``<parameters>`` element** of a vasprun.xml —
which sits before the ``<calculation>`` ionic-step blocks — so it is cheap regardless of the
trajectory length (validated: identical output to a full ``Vasprun`` parse, ~10x faster on a
small file and unboundedly faster on a long AIMD run whose trajectory it never reads).

Two design choices keep it faithful and consistent:

* **pymatgen parses the block, not us.** The ``<parameters>`` element is handed to pymatgen's
  own ``Vasprun._parse_params`` (via a bare ``__new__`` instance — that method only recurses on
  itself and reads ``self.filename``), so the values, types, and the ``response functions``
  dedup quirk match ``vasprun.parameters`` exactly. Keep in step with pymatgen on upgrade (a
  divergence would only mis-store a parameter value, which the offline test guards).
* **One schema across parsers.** ``vasprun.xml``'s ``<parameters>`` names the plane-wave cutoff
  ``ENMAX`` (VASP's internal name); the INCAR and the OUTCAR header block call it ``ENCUT``. We
  rename ``ENMAX``\\ →\\ ``ENCUT`` (:data:`_PARAM_ALIASES`) and restrict to the shared
  :data:`~zenodo_harvest.outcar_params.EFFECTIVE_TAGS`, so the vasprun and OUTCAR ``parameters``
  blocks are keyed identically and the resolver reads ``ENCUT`` from either.

``resolved_parameters`` (the rename + restrict + JSON-coerce) is a pure function shared with the
live parser (:func:`parse._calc_parameters`), so a freshly-parsed record and a recovered one
produce a byte-identical ``parameters`` block.
"""

from __future__ import annotations

import bz2
import gzip
import lzma
import logging
from pathlib import Path
from typing import Any

from .outcar_params import EFFECTIVE_TAGS

logger = logging.getLogger(__name__)

# Compressed-primary openers (binary, for lxml). VASP outputs are staged verbatim, so a
# ``vasprun.xml`` may arrive gzip/bzip2/xz/lzma-compressed — the same set the OUTCAR header
# reader handles. Feeding a still-compressed file to the XML parser fails with a bogus
# "Start tag expected" at line 1, so every suffix here MUST be decompressed on the way in.
_OPENERS: dict[str, Any] = {".gz": gzip.open, ".bz2": bz2.open,
                            ".xz": lzma.open, ".lzma": lzma.open}

# vasprun.xml's <parameters> block internal name -> the canonical INCAR name we store under
# (EFFECTIVE_TAGS is keyed by INCAR names). Only the cutoff differs between the two.
_PARAM_ALIASES = {"ENMAX": "ENCUT"}


def _jsonable(obj: Any) -> Any:
    """Coerce numpy / pymatgen values into JSON-serialisable Python (numpy imported lazily so
    this module stays importable — and ``resolved_parameters`` unit-testable — without it)."""
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    try:
        import numpy as np
        if isinstance(obj, np.bool_):          # before np.integer: keep JSON true/false
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return _jsonable(obj.tolist())
    except ImportError:
        pass
    return str(obj)                            # enums, etc.


def resolved_parameters(params: Any) -> dict[str, Any]:
    """A raw vasprun ``parameters`` mapping -> the stored ``parameters`` block.

    Renames vasprun aliases (``ENMAX``\\ →\\ ``ENCUT``), restricts to
    :data:`~zenodo_harvest.outcar_params.EFFECTIVE_TAGS`, and JSON-coerces. Pure and shared by
    :func:`parse._calc_parameters` (fed ``vasprun.parameters``) and the vasprun recovery (fed a
    header-parsed dict), so both emit a byte-identical block.
    """
    src = dict(params or {})
    for alias, canonical in _PARAM_ALIASES.items():
        if src.get(canonical) is None and src.get(alias) is not None:
            src[canonical] = src[alias]
    return {k: _jsonable(src[k]) for k in EFFECTIVE_TAGS if src.get(k) is not None}


def parse_vasprun_parameters(path: str | Path) -> dict[str, Any] | None:
    """Read ONLY the ``<parameters>`` element of a vasprun.xml -> :func:`resolved_parameters`.

    Streams the file with ``iterparse`` and stops at the first ``<calculation>`` start event
    (the ionic trajectory), so a multi-GB AIMD vasprun costs the same as a single-point one.
    Parses the block with pymatgen's own ``_parse_params`` for exact parity with
    ``vasprun.parameters``. Handles plain or gzip/bzip2/xz/lzma files. Returns ``None`` if the
    file is unreadable or carries no ``<parameters>`` block (caller leaves the record as-is).
    """
    from lxml import etree                                   # pymatgen's own XML backend
    from pymatgen.io.vasp.outputs import Vasprun

    p = str(path)
    low = p.lower()
    opener = next((fn for suf, fn in _OPENERS.items() if low.endswith(suf)), open)
    try:
        with opener(p, "rb") as fh:
            for event, elem in etree.iterparse(fh, events=("start", "end"), huge_tree=True):
                if event == "start" and elem.tag == "calculation":
                    break                                    # header done; never read the body
                if event == "end" and elem.tag == "parameters":
                    dummy = Vasprun.__new__(Vasprun)         # _parse_params only needs .filename
                    dummy.filename = p
                    return resolved_parameters(dict(dummy._parse_params(elem)))
    except Exception as exc:                                 # noqa: BLE001 - any XML/parse issue
        logger.warning("cannot read vasprun parameters from %s: %s", path, exc)
        return None
    return None
