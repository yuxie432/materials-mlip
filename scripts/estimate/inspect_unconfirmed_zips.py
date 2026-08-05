#!/usr/bin/env python3
"""Inspect a sample of ``zip_unconfirmed``-bucket records that fetch rejected
``no_vasp_files_fetched`` — to decide whether their content is genuinely non-VASP
(so bucket 3's low yield is a bad *assumption*, not a bug) or whether a VASP primary
was actually missed (a *recoverable* bug).

It reuses the pipeline's OWN name classifiers (``models._VASP_RE`` / ``fetch._PARSE_RE`` /
``_is_junk_member`` / ``_nested_archive_kind``) so a verdict here is exactly what
``fetch`` would decide. Method, cheapest-first:

  1. Remote-peek each zip's central directory (~tens of KB, no download). That alone
     classifies a record when the top level is foreign files or VASP inputs.
  2. Only when the peek reveals a *nested archive* (or the peek fails) is the whole zip
     downloaded (capped by ``--max-mb``) and walked recursively — opening nested
     ``.zip``/``.tar*`` in memory — so we can see whether a vasprun/OUTCAR hides inside.

Staging was purged, so step 2 re-downloads; run it on a login node (network + light CPU).

    python scripts/estimate/inspect_unconfirmed_zips.py --n 15 --max-mb 300
"""
from __future__ import annotations

import argparse
import collections
import io
import json
import os
import random
import sys
import tarfile
import time
import zipfile
from pathlib import Path

# Import the package when run from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests

from zenodo_harvest import config
from zenodo_harvest.fetch import _PARSE_RE, _is_junk_member, _nested_archive_kind
from zenodo_harvest.models import VASP_PRIMARY, _VASP_RE
from zenodo_harvest.triage import peek_zip_filenames


def load_jsonl(path: Path):
    with open(path) as fh:
        for ln in fh:
            ln = ln.strip()
            if ln:
                try:
                    yield json.loads(ln)
                except json.JSONDecodeError:
                    pass  # tolerate a torn final line on a live job


def is_primary(base: str) -> bool:
    """A parseable VASP primary output (vasprun/OUTCAR/vaspout), by triage's own regex."""
    if _is_junk_member(base):
        return False
    m = _VASP_RE.search(base)
    return bool(m) and m.group(1).lower() in VASP_PRIMARY


def is_vasp_name(base: str) -> bool:
    """Any VASP file name fetch would keep (primary or input), not junk."""
    return not _is_junk_member(base) and bool(_PARSE_RE.search(base))


def open_archive(src, kind: str):
    """Return (infos, reader) for an archive. ``src`` is a path (outer, read on demand)
    or bytes (nested, in memory). infos = [(name, size)]; reader(name) -> bytes."""
    if kind == "zip":
        zf = zipfile.ZipFile(src if isinstance(src, (str, Path)) else io.BytesIO(src))
        infos = [(i.filename, i.file_size) for i in zf.infolist() if not i.is_dir()]
        return infos, zf.read
    # tar family: tarfile auto-detects gz/bz2/xz from content.
    tf = (tarfile.open(name=src) if isinstance(src, (str, Path))
          else tarfile.open(fileobj=io.BytesIO(src)))
    members = [m for m in tf.getmembers() if m.isfile()]
    by_name = {m.name: m for m in members}
    return ([(m.name, m.size) for m in members],
            lambda name: tf.extractfile(by_name[name]).read())


def walk(src, kind: str, depth: int, max_member_mb: int, prefix: str = ""):
    """Yield every leaf member path in an archive, recursing into nested zip/tar."""
    if depth <= 0:
        yield prefix + "<max-depth>"
        return
    try:
        infos, reader = open_archive(src, kind)
    except Exception as exc:  # noqa: BLE001 - a corrupt/odd archive is data, not a crash
        yield prefix + f"<unreadable {kind}: {type(exc).__name__}: {exc}>"
        return
    for name, size in infos:
        if _is_junk_member(name):
            continue
        base = name.rsplit("/", 1)[-1]
        nk = _nested_archive_kind(base)
        if nk in ("zip", "tar"):  # openable here with the stdlib
            if size <= max_member_mb * 1024 * 1024:
                try:
                    yield from walk(reader(name), nk, depth - 1, max_member_mb,
                                    prefix + name + "!/")
                    continue
                except Exception as exc:  # noqa: BLE001
                    yield prefix + name + f" <nested {nk} unreadable: {type(exc).__name__}>"
                    continue
            yield prefix + name + f" <nested {nk}, not opened ({size/1e6:.0f} MB)>"
        elif nk is not None:  # rar/7z/tar.zst: needs a backend the stdlib lacks
            yield prefix + name + f" <nested {nk}, not opened (no stdlib reader)>"
        else:
            yield prefix + name


def ext_histogram(names, top=8) -> str:
    c = collections.Counter(
        (n.rsplit("!/", 1)[-1].rsplit("/", 1)[-1].rsplit(".", 1)[-1][:8].lower()
         if "." in n.rsplit("/", 1)[-1] else "<noext>")
        for n in names if not n.lstrip().startswith("<"))
    return "  ".join(f".{e}×{k}" for e, k in c.most_common(top))


def download(url: str, dest: Path, cap_bytes: int, session: requests.Session) -> bool:
    got = 0
    with session.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
                got += len(chunk)
                if got > cap_bytes:
                    return False  # over cap -> abandon
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    data = os.environ.get("ZENODO_HARVEST_DATA", "data")
    ap.add_argument("--manifests", default=str(Path(data) / "manifests"))
    ap.add_argument("--n", type=int, default=15, help="sample size (default 15)")
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for a reproducible sample")
    ap.add_argument("--max-mb", type=int, default=300,
                    help="per-zip download cap; bigger nested-archive zips are peeked only")
    ap.add_argument("--depth", type=int, default=4, help="nested-archive recursion depth")
    ap.add_argument("--reason", default="no_vasp_files_fetched",
                    help="fetch rejection reason to sample (default no_vasp_files_fetched)")
    args = ap.parse_args()

    config.load_dotenv()
    man = Path(args.manifests)

    keep = {c["recid"]: c for c in load_jsonl(man / "keep.jsonl")}
    # recids fetch rejected with the target reason, at RECORD level (id has no ':').
    rejected = {
        str(r.get("id")) for r in load_jsonl(man / "rejections.jsonl")
        if r.get("stage") == "fetch" and r.get("reason") == args.reason
        and isinstance(r.get("id"), str) and ":" not in r["id"]
    }

    def is_zip_unconfirmed(c) -> bool:
        names = [a.lower() for a in c.get("archives", [])]
        return (not c.get("primary_vasp_files") and c.get("vasp_category") == "archive"
                and bool(names) and all(n.endswith(".zip") for n in names))

    pool = [keep[r] for r in rejected if r in keep and is_zip_unconfirmed(keep[r])]
    print(f"# {len(pool)} zip_unconfirmed records rejected '{args.reason}' "
          f"(of {len(rejected)} total {args.reason}); sampling {min(args.n, len(pool))}\n")
    if not pool:
        return 0
    random.seed(args.seed)
    sample = random.sample(pool, min(args.n, len(pool)))

    session = requests.Session()
    session.headers["User-Agent"] = "zenodo-harvest/inspect"
    if os.environ.get("ZENODO_TOKEN"):
        session.headers["Authorization"] = f"Bearer {os.environ['ZENODO_TOKEN']}"

    verdicts: collections.Counter = collections.Counter()
    tmp = Path(os.environ.get("TMPDIR", "/tmp")) / "zh_inspect"
    tmp.mkdir(parents=True, exist_ok=True)

    for c in sample:
        zips = [f for f in c["files"]
                if (f.get("key", "").lower().endswith(".zip")) and f.get("download")]
        title = (c.get("title") or "")[:70]
        mb = sum(f.get("size") or 0 for f in zips) / 1e6
        print(f"── recid {c['recid']}  ({mb:.0f} MB, {len(zips)} zip)  {title}")
        print(f"   {c.get('zenodo_url')}")

        all_names: list[str] = []
        found_primary: list[str] = []
        found_input = False
        needs_download = False

        for f in zips:
            top = peek_zip_filenames(f["download"], session)
            time.sleep(1.0)  # peek shares Zenodo's 30/min search budget
            if top is None:
                needs_download = True
                print(f"   peek FAILED on {f['key']} (ZIP64/too-small/error) -> will download")
                continue
            bases = [n.rsplit("/", 1)[-1] for n in top]
            nested = [n for n in top if _nested_archive_kind(n.rsplit('/', 1)[-1])]
            found_primary += [n for n in top if is_primary(n.rsplit("/", 1)[-1])]
            found_input = found_input or any(is_vasp_name(b) for b in bases)
            all_names += top
            print(f"   peek {f['key']}: {len(top)} members  top-exts: {ext_histogram(top)}")
            if nested:
                needs_download = True
                print(f"     nested archive(s): {nested[:5]}")

        # Only download when the answer is hidden (nested archive) or the peek failed,
        # and no primary is already visible at the top level.
        if needs_download and not found_primary:
            for f in zips:
                if (f.get("size") or 0) > args.max_mb * 1024 * 1024:
                    print(f"   SKIP download {f['key']}: {f['size']/1e6:.0f} MB > "
                          f"{args.max_mb} MB cap — peeked top-level only")
                    continue
                arc = tmp / f"{c['recid']}_{f['key'].rsplit('/', 1)[-1]}"
                try:
                    if not download(f["download"], arc, args.max_mb * 1024 * 1024, session):
                        print(f"   download exceeded {args.max_mb} MB cap — abandoned")
                        arc.unlink(missing_ok=True)
                        continue
                    leaves = list(walk(arc, "zip", args.depth, args.max_mb))
                    all_names = leaves  # full recursive listing supersedes the peek
                    found_primary = [n for n in leaves if is_primary(n.rsplit("/", 1)[-1].rsplit("!/", 1)[-1])]
                    found_input = found_input or any(
                        is_vasp_name(n.rsplit("/", 1)[-1]) for n in leaves)
                    print(f"   walked {f['key']}: {len(leaves)} leaves  "
                          f"exts: {ext_histogram(leaves)}")
                finally:
                    arc.unlink(missing_ok=True)

        if found_primary:
            verdict = "MISSED_PRIMARY  <-- fetch should have kept this"
            verdicts["MISSED_PRIMARY"] += 1
            for p in found_primary[:5]:
                print(f"     !! primary present: {p}")
        elif any("not opened" in n or n.lstrip().startswith("<") for n in all_names):
            verdict = "INCONCLUSIVE (unopened nested/too-big — inspect by hand)"
            verdicts["INCONCLUSIVE"] += 1
        elif found_input:
            verdict = "INPUT_ONLY (VASP inputs but no primary — correctly rejected)"
            verdicts["INPUT_ONLY"] += 1
        else:
            verdict = "FOREIGN (no VASP name anywhere — correctly rejected)"
            verdicts["FOREIGN"] += 1
        # A couple of example names to eyeball the domain.
        egs = [n for n in all_names if not n.lstrip().startswith("<")][:4]
        print(f"   VERDICT: {verdict}")
        if egs:
            print(f"   e.g.: {egs}")
        print()

    print("=" * 60)
    print("SUMMARY:", dict(verdicts))
    print("MISSED_PRIMARY>0 => a recoverable fetch bug; else bucket-3's low yield is real.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
