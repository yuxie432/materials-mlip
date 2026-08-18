"""Retrofit net moment/charge + OUTCAR SCF convergence onto an already-built dataset (shard-rewriting).

The first harvests stored neither the net moment nor the net charge, and stored per-atom
``dft_magmom``/``dft_charge`` on the final frame of vasprun+OUTCAR calcs; and OUTCAR-parsed calcs
carried no per-frame ``scf_dE``/``electronic_converged`` (the old OUTCAR path could not read the SCF
trace). This module brings an existing dataset to the new schema in ONE shard pass:

* every frame gains ``total_magnetization`` / ``total_charge`` (:mod:`electronic`), the per-atom
  arrays are stripped, and each metadata record gains the ``electronic`` block (``site_*_present``
  removed);
* every frame of a calc whose SCF trace lives in an OUTCAR — the OUTCAR reader
  (``parser == "ase.OUTCAR"``) **and** a ``pymatgen.Vaspout`` calc with a co-located OUTCAR
  (vaspout.h5 carries no per-SCF data, so a fresh ``parse_vaspout`` fills it from the OUTCAR too) —
  additionally gains that ionic step's own ``scf_dE`` (free-energy basis) + ``electronic_converged``
  from the OUTCAR trace (:mod:`convergence`), and its metadata ``quality`` gets the final-step
  verdict + ``n_frames_scf_unconverged`` — vasprun-level parity. vasprun calcs already have σ→0
  ``scf_dE`` from the live parse, so they are left untouched here.

Unlike the OUTCAR / vasprun / availability recoveries (metadata-only, shards untouched), this one
**rewrites shards** — the values live on the frames — so it is deliberately a separate,
clearly-labelled tool.

A shard interleaves frames from *many* calcs, so it cannot be rewritten until every one of its
calcs' net values is known. Hence **two phases**:

1. :func:`compute_net_properties` (Phase 1, disk-paced) — re-fetch the records (the ordinary
   ``fetch`` on :func:`build_net_properties_keeplist`'s keep-list) and, for each re-fetched
   calc, compute its net moment/charge (:func:`parse.electronic_block_for_unit`, the SAME code
   a fresh parse uses) and — for calcs whose SCF trace lives in an OUTCAR (``ase.OUTCAR`` and
   ``pymatgen.Vaspout``-with-OUTCAR) — its per-frame SCF convergence
   (:func:`parse.convergence_block_for_unit`) into a ``net_properties.jsonl`` map
   (``calc_id`` → ``{electronic, convergence?}``). Resumable: calcs already in the map are
   skipped, so the fetch→compute→purge loop can pace against the disk quota exactly like the
   other recoveries. No shard is touched here.
2. :func:`apply_net_properties` (Phase 2, local, no network) — drive the map into the dataset:
   a **text-level** shard edit (append the two totals to each frame's comment line, strip the
   ``dft_magmom``/``dft_charge`` columns, and — for OUTCAR frames — append that step's
   ``scf_dE``/``electronic_converged``) that leaves every untouched frame — and every kept
   value — **byte-identical** (no ASE re-serialisation, so no float-precision drift), then an
   atomic metadata rewrite adding ``electronic`` (dropping ``site_*_present``) and, for OUTCAR
   calcs, overwriting the ``quality`` convergence fields. Idempotent (re-running strips-then-
   re-appends the keys and re-strips absent columns → no-op) and resumable (a
   ``.net_properties_applied`` marker skips shards already done).

``verify`` still passes afterwards: no frame is added or removed and ``shards``/``frame_ids``/
``calc_id`` are untouched — only per-frame info/arrays content and the metadata ``electronic``
block + ``quality`` convergence fields change. The design remains open to a further per-frame
addition (e.g. an OUTCAR ionic-convergence ΔE): another Phase-1 field + the same Phase-2 edit.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any

from .convergence import converged_ionic_from_params, cparam
from .manifest import JsonlWriter, read_jsonl
from .store import DatasetLock, existing_shard_paths

logger = logging.getLogger(__name__)

_REFRESH_BACKUP = "metadata.jsonl.bak.pre_net_properties"
_APPLIED_MARKER = ".net_properties_applied"          # shard basenames already rewritten (Phase 2)

# calc_id in a comment line is quoted (contains ':' / '/' / '#'); fall back to a bare token.
_CALC_ID_RE = re.compile(r'calc_id=(?:"([^"]*)"|(\S+))')
_PROPS_RE = re.compile(r"Properties=(\S+)")
# existing total_* keys, stripped before re-appending so Phase 2 is idempotent.
_TOTAL_KEY_RE = re.compile(r'\s*total_(?:magnetization|charge)=(?:"[^"]*"|\S+)')
# existing per-frame convergence keys, stripped before re-appending (idempotency); the ORIGINAL
# OUTCAR path wrote neither, so the first apply just appends, and a re-apply strips+re-appends.
_CONV_KEY_RE = re.compile(r'\s*(?:electronic_converged|scf_dE)=(?:"[^"]*"|\S+)')
_IONIC_STEP_RE = re.compile(r'\bionic_step=(\d+)')
_DFT_COLS = {"dft_magmom", "dft_charge"}


# --- Phase 0: keep-list ----------------------------------------------------------------

def _dataset_recids(metadata_path: Path) -> set[str]:
    """Every ``provenance.record_id`` that produced a dataset calc (net props affect all)."""
    recids: set[str] = set()
    for rec in read_jsonl(metadata_path):
        recid = (rec.get("provenance") or {}).get("record_id")
        if recid:
            recids.add(str(recid))
    return recids


def build_net_properties_keeplist(dataset_dir: str | Path, keep_path: str | Path,
                                  out_path: str | Path) -> dict:
    """Emit the keep-list of every record holding a dataset calc, to re-fetch for Phase 1.

    Net moment/charge are computed for *every* calc, so (like the availability recovery) this
    selects every record in ``metadata.jsonl`` and writes its full ``keep.jsonl`` entry (with
    file URLs). A record absent from ``keep_path`` is reported under ``missing_from_keep``.
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}

    recids = _dataset_recids(metadata_path)
    keep_by_recid = {str(rec.get("recid")): rec for rec in read_jsonl(keep_path)}
    written, bytes_total, missing = 0, 0, []
    with JsonlWriter(out_path) as out:
        for recid in sorted(recids):
            entry = keep_by_recid.get(recid)
            if entry is None:
                missing.append(recid)
                continue
            out.write(entry)
            written += 1
            bytes_total += int(entry.get("bytes_total") or 0)
    if missing:
        logger.warning("%d recid(s) absent from %s — cannot re-fetch without their file URLs: "
                       "%s%s", len(missing), keep_path, missing[:10],
                       " ..." if len(missing) > 10 else "")
    return {
        "ok": True,
        "dataset_dir": str(dataset_dir),
        "out_path": str(out_path),
        "records_targeted": len(recids),
        "records_written": written,
        "gib_to_refetch": round(bytes_total / 2**30, 2),
        "missing_from_keep": missing,
    }


# --- Phase 1: compute the net-properties map (re-fetch, no shard access) ----------------

def compute_net_properties(fetched: str | Path, raw_dir: str | Path,
                           out_path: str | Path,
                           dataset_dir: str | Path | None = None) -> dict:
    """Compute each re-fetched calc's net moment/charge into a ``net_properties.jsonl`` map.

    ``fetched`` is the ``fetched.jsonl`` from re-fetching the keep-list; ``raw_dir`` where it
    staged. For each calc unit whose primary is present (and, if ``dataset_dir`` is given, whose
    ``calc_id`` names a dataset record), the ``electronic`` block is computed
    (:func:`parse.electronic_block_for_unit`) and appended as ``{"calc_id", "electronic"}``.
    Resumable: calc_ids already in ``out_path`` are skipped, so the batched
    fetch→compute→purge loop resumes cleanly. Appends only — never rewrites shards.
    """
    from .parse import (_calc_id, _resolve, convergence_block_for_unit,
                        electronic_block_for_unit)

    fetched, raw_dir, out_path = Path(fetched), Path(raw_dir), Path(out_path)
    if not fetched.is_file():
        return {"ok": False, "error": f"no fetched manifest at {fetched}"}

    # Convergence recovery is scoped to calcs whose SCF trace lives in an OUTCAR — the OUTCAR
    # reader (ase.OUTCAR) AND vaspout (whose HDF5 carries no per-SCF data, so a fresh parse_vaspout
    # fills it from a co-located OUTCAR). A vasprun already has σ→0 scf_dE from the live parse, so
    # it is skipped. Load each dataset calc's parser to decide; both maps come from one metadata read.
    dataset_calc_ids: set[str] | None = None
    parser_by_calc_id: dict[str, str] = {}
    if dataset_dir is not None:
        mp = Path(dataset_dir) / "metadata.jsonl"
        dataset_calc_ids = set()
        if mp.is_file():
            for r in read_jsonl(mp):
                cid = r.get("calc_id")
                if cid:
                    dataset_calc_ids.add(cid)
                    if r.get("parser"):
                        parser_by_calc_id[cid] = r["parser"]

    done = {r["calc_id"] for r in read_jsonl(out_path) if r.get("calc_id")} if out_path.is_file() else set()
    n_computed = n_skipped = n_absent = 0
    with JsonlWriter(out_path) as out:
        for rec in read_jsonl(fetched):
            base_meta = {"provenance": rec["provenance"],
                         "_extracted_root": str(_resolve(raw_dir, rec["local_dir"]) / "extracted")}
            for unit in rec.get("calc_units", []):
                unit_abs = {k: str(_resolve(raw_dir, v)) for k, v in unit.items()}
                calc_id = _calc_id(unit_abs, base_meta)
                if calc_id in done:
                    n_skipped += 1
                    continue
                if dataset_calc_ids is not None and calc_id not in dataset_calc_ids:
                    continue
                primary = unit_abs.get("vasprun") or unit_abs.get("vaspout") or unit_abs.get("outcar")
                if not primary or not os.path.exists(primary):
                    n_absent += 1                 # purged/unstaged -> a later batch handles it
                    continue
                entry: dict[str, Any] = {"calc_id": calc_id,
                                         "electronic": electronic_block_for_unit(unit_abs)}
                # OUTCAR-parsed calcs — and vaspout calcs with a co-located OUTCAR — also get
                # per-frame SCF convergence from the OUTCAR trace (a fresh parse_vaspout fills it
                # from the OUTCAR too, since vaspout.h5 has no per-SCF data). vasprun calcs already
                # carry σ→0 scf_dE from the live parse, so they are skipped (no convergence entry).
                # convergence_block_for_unit returns None when the unit has no staged OUTCAR (e.g.
                # a vaspout-only calc), leaving those frames' convergence null — matching a fresh parse.
                if parser_by_calc_id.get(calc_id) in ("ase.OUTCAR", "pymatgen.Vaspout"):
                    conv = convergence_block_for_unit(unit_abs)
                    if conv is not None:
                        entry["convergence"] = conv
                out.write(entry)
                done.add(calc_id)
                n_computed += 1
    return {
        "ok": True,
        "out_path": str(out_path),
        "computed_this_pass": n_computed,
        "skipped_already_computed": n_skipped,
        "skipped_absent_primary": n_absent,
        "map_size": len(done),
    }


# --- Phase 2: apply the map (text-level shard rewrite + metadata rewrite) ----------------

def _strip_dft_columns(comment: str, atom_rows: list[str]) -> tuple[str, list[str], bool]:
    """Remove the ``dft_magmom``/``dft_charge`` columns from a frame's Properties + atom rows.

    Kept-column token strings are preserved verbatim (only the two per-atom output columns are
    dropped), so no value is reformatted. Returns ``(comment, rows, changed)`` unchanged when the
    frame has no such columns (the common case: only final frames of vasprun+OUTCAR calcs had
    them).
    """
    m = _PROPS_RE.search(comment)
    if not m:
        return comment, atom_rows, False
    parts = m.group(1).split(":")
    if len(parts) % 3 != 0:
        return comment, atom_rows, False
    triples = [(parts[i], parts[i + 1], parts[i + 2]) for i in range(0, len(parts), 3)]
    if not any(name in _DFT_COLS for name, _t, _c in triples):
        return comment, atom_rows, False
    drop: set[int] = set()
    kept: list[str] = []
    offset = 0
    for name, typ, cnt in triples:
        try:
            n = int(cnt)
        except ValueError:
            return comment, atom_rows, False
        if name in _DFT_COLS:
            drop.update(range(offset, offset + n))
        else:
            kept.append(f"{name}:{typ}:{cnt}")
        offset += n
    new_comment = comment[:m.start(1)] + ":".join(kept) + comment[m.end(1):]
    new_rows = [" ".join(t for i, t in enumerate(row.split()) if i not in drop) for row in atom_rows]
    return new_comment, new_rows, True


def _append_totals(comment: str, block: dict) -> str:
    """Append ``total_magnetization``/``total_charge`` to a comment line (idempotently)."""
    comment = _TOTAL_KEY_RE.sub("", comment).rstrip()
    add = []
    nm, nc = block.get("net_magnetization"), block.get("net_charge")
    if nm is not None:
        add.append(f"total_magnetization={float(nm)}")
    if nc is not None:
        add.append(f"total_charge={float(nc)}")
    return (comment + " " + " ".join(add)) if add else comment


def _append_convergence(comment: str, conv_block: dict) -> str:
    """Append this frame's ``scf_dE``/``electronic_converged`` from an OUTCAR convergence block.

    Idempotent: strips any existing keys first (the original OUTCAR path wrote neither, so a
    first apply just appends). The frame's ``ionic_step`` (read from the comment) selects its own
    step's ``[scf_dE, electronic_converged]`` from ``conv_block["per_step"]``; a frame whose step
    is absent (or has no ``ionic_step``) is left with the keys stripped (unknown). Bools are
    written ``T``/``F`` to match ASE's extxyz serialisation exactly (verified on read-back).
    """
    comment = _CONV_KEY_RE.sub("", comment).rstrip()
    m = _IONIC_STEP_RE.search(comment)
    if not m:
        return comment
    step = (conv_block.get("per_step") or {}).get(m.group(1))
    if not step:
        return comment
    dE, converged = step[0], step[1]
    add = []
    if converged is not None:
        add.append("electronic_converged=" + ("T" if converged else "F"))
    if dE is not None:
        add.append(f"scf_dE={float(dE)}")
    return (comment + " " + " ".join(add)) if add else comment


def _rewrite_shard_text(shard: Path, entries: dict[str, dict]) -> dict:
    """Text-edit one shard's frames in place (atomic). Strips per-atom dft columns from every
    frame that has them, appends the two totals to frames whose calc_id is in ``entries``, and —
    for an entry carrying a ``convergence`` block (OUTCAR calcs) — appends that frame's own
    ``scf_dE``/``electronic_converged``.

    ``entries`` maps ``calc_id`` → ``{"electronic": block, "convergence": block?}``. A vasprun
    calc has no ``convergence`` entry, so its existing per-frame convergence keys are never
    touched. Reads the whole (intact) shard text and walks extxyz frames. If any frame is
    malformed the shard is left UNTOUCHED (returns ``ok=False``) rather than risk corrupting it —
    safer than a partial rewrite, and the recovery can be re-run. Only rewrites when something
    changed.
    """
    try:
        with gzip.open(shard, "rt") as fh:
            lines = fh.read().splitlines()
    except OSError as exc:
        logger.warning("cannot read shard %s: %s", shard.name, exc)
        return {"ok": False, "frames": 0, "changed": 0}

    out: list[str] = []
    n_frames = n_changed = 0
    i = 0
    while i < len(lines):
        try:
            natoms = int(lines[i].strip())
        except (ValueError, IndexError):
            logger.warning("malformed frame in %s at line %d; leaving shard untouched",
                           shard.name, i)
            return {"ok": False, "frames": n_frames, "changed": n_changed}
        if i + 2 + natoms > len(lines):
            logger.warning("truncated final frame in %s; leaving shard untouched", shard.name)
            return {"ok": False, "frames": n_frames, "changed": n_changed}
        count_line, comment = lines[i], lines[i + 1]
        atom_rows = lines[i + 2:i + 2 + natoms]
        n_frames += 1

        comment2, rows2, stripped = _strip_dft_columns(comment, atom_rows)
        m = _CALC_ID_RE.search(comment2)
        calc_id = (m.group(1) if m and m.group(1) is not None else (m.group(2) if m else None))
        appended = False
        entry = entries.get(calc_id) if calc_id is not None else None
        if entry is not None:
            new_comment = _append_totals(comment2, entry["electronic"])
            conv = entry.get("convergence")
            if conv is not None:
                new_comment = _append_convergence(new_comment, conv)
            appended = new_comment != comment2
            comment2 = new_comment
        if stripped or appended:
            n_changed += 1
        out.append(count_line)
        out.append(comment2)
        out.extend(rows2)
        i += 2 + natoms

    if n_changed == 0:
        return {"ok": True, "frames": n_frames, "changed": 0}
    tmp = shard.with_suffix(shard.suffix + ".np.tmp")
    with gzip.open(tmp, "wt") as fh:
        fh.write("\n".join(out) + "\n")
    tmp.replace(shard)                                # atomic
    return {"ok": True, "frames": n_frames, "changed": n_changed}


def _load_applied(marker: Path) -> set[str]:
    return set(marker.read_text().split()) if marker.is_file() else set()


def apply_net_properties(dataset_dir: str | Path, net_properties: str | Path,
                         dry_run: bool = False, backup: bool = True) -> dict:
    """Phase 2: drive the ``net_properties.jsonl`` map into the dataset (shards + metadata).

    Rewrites each shard (text-level, atomic; skips those recorded done in ``.net_properties_applied``)
    to add ``total_magnetization``/``total_charge`` per frame, strip the per-atom dft columns, and —
    for OUTCAR calcs — append each frame's ``scf_dE``/``electronic_converged``; then rewrites
    ``metadata.jsonl`` (atomic, one-time ``metadata.jsonl.bak.pre_net_properties`` backup) to add
    each calc's ``electronic`` block (dropping ``site_*_present``) and, for OUTCAR calcs, overwrite
    the ``quality`` convergence fields. Held under the dataset ``.parse.lock``. ``dry_run`` reports
    what would change and writes nothing.
    """
    dataset_dir = Path(dataset_dir)
    metadata_path = dataset_dir / "metadata.jsonl"
    if not metadata_path.is_file():
        return {"ok": False, "error": f"no metadata.jsonl in {dataset_dir}"}
    if not Path(net_properties).is_file():
        return {"ok": False, "error": f"no net-properties map at {net_properties}"}

    # calc_id -> {"electronic": block, "convergence": block?}. The convergence block is present
    # only for OUTCAR calcs (Phase 1 computed it there); vasprun calcs carry electronic only.
    entries: dict[str, dict] = {}
    for rec in read_jsonl(net_properties):
        cid, blk = rec.get("calc_id"), rec.get("electronic")
        if cid and isinstance(blk, dict):
            entry: dict[str, Any] = {"electronic": blk}
            conv = rec.get("convergence")
            if isinstance(conv, dict) and isinstance(conv.get("per_step"), dict):
                entry["convergence"] = conv
            entries[cid] = entry

    marker = dataset_dir / _APPLIED_MARKER
    stats: dict[str, Any] = {"ok": True, "dataset_dir": str(dataset_dir), "dry_run": dry_run,
                             "map_size": len(entries), "shards_total": 0, "shards_rewritten": 0,
                             "shards_skipped_done": 0, "shards_failed": 0, "frames_changed": 0,
                             "metadata_records": 0, "records_refreshed": 0,
                             "ionic_converged_set": 0,
                             "convergence_records": sum(1 for e in entries.values()
                                                        if "convergence" in e)}

    if dry_run:
        stats["shards_total"] = len(existing_shard_paths(dataset_dir))
        for record in read_jsonl(metadata_path):
            stats["metadata_records"] += 1
            if record.get("calc_id") in entries:
                stats["records_refreshed"] += 1
        return stats

    with DatasetLock(dataset_dir):
        # Phase 2a: shards.
        applied = _load_applied(marker)
        shards = existing_shard_paths(dataset_dir)
        stats["shards_total"] = len(shards)
        with marker.open("a") as mk:
            for shard in shards:
                if shard.name in applied:
                    stats["shards_skipped_done"] += 1
                    continue
                res = _rewrite_shard_text(shard, entries)
                if not res["ok"]:
                    stats["shards_failed"] += 1
                    continue
                if res["changed"]:
                    stats["shards_rewritten"] += 1
                    stats["frames_changed"] += res["changed"]
                mk.write(shard.name + "\n")           # done (even if 0 changed → won't reprocess)
                mk.flush()
                os.fsync(mk.fileno())

        # Phase 2b: metadata (atomic rewrite, one-time backup).
        def _apply(record: dict) -> dict:
            cid = record.get("calc_id")
            entry = entries.get(cid) if cid is not None else None
            if entry is not None:
                record["electronic"] = entry["electronic"]
                record.pop("site_magmoms_present", None)
                record.pop("site_charges_present", None)
                conv = entry.get("convergence")
                if conv is not None and isinstance(record.get("quality"), dict):
                    # OUTCAR calc: overwrite the final-step verdict + n_frames_scf_unconverged
                    # (were electronic_converged=None/scf_dE=None before this recovery).
                    record["quality"].update(conv["quality"])
                stats["records_refreshed"] += 1
            # Calc-level ionic_converged parity for OUTCAR calcs — computed from the record's OWN
            # metadata (calc_parameters NSW/IBRION/EDIFFG + quality.n_ionic_steps), so it needs no
            # re-fetch and covers EVERY OUTCAR record (even ones absent from the map). Fills the
            # gap only (was None; vasprun/vaspout already have it from pymatgen).
            q = record.get("quality")
            if (record.get("parser") == "ase.OUTCAR" and isinstance(q, dict)
                    and q.get("ionic_converged") is None):
                cp = record.get("calc_parameters") or {}
                ic = converged_ionic_from_params(cparam(cp, "NSW"), cparam(cp, "IBRION"),
                                                 cparam(cp, "EDIFFG"), q.get("n_ionic_steps"))
                if ic is not None:
                    q["ionic_converged"] = ic
                    stats["ionic_converged_set"] += 1
            return record

        tmp = metadata_path.parent / (metadata_path.name + ".np.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for record in read_jsonl(metadata_path):
                stats["metadata_records"] += 1
                fh.write(json.dumps(_apply(record)) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        bak = metadata_path.parent / _REFRESH_BACKUP
        if backup and not bak.exists():
            shutil.copy2(metadata_path, bak)
        os.replace(tmp, metadata_path)

    return stats
