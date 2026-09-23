"""Live end-to-end smoke test of the Materials Cloud harvest, in an isolated work dir.

Runs the REAL path on a few small records — discover (by id) → triage (real central-directory
peeks over MC's 302→S3 redirect) → the shared fetch (anonymous session) → the shared pymatgen/ASE
parse → store → verify — and prints PASS/FAIL checks. Never touches a production tree (everything
is written under ``work``, deleted afterwards unless ``keep``). Default record: ``ydn09-ngs56``
(16.5 MB zip of HSE06 OUTCARs → exercises the ASE OUTCAR parser + peek-confirmed zip triage).

    python -m materials_cloud_harvest.cli smoke                      # default record
    python -m materials_cloud_harvest.cli smoke --record <id> --keep  # any record(s)
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from zenodo_harvest.dataset_ops import verify_dataset
from zenodo_harvest.fetch import fetch as shared_fetch
from zenodo_harvest.manifest import read_jsonl
from zenodo_harvest.parse import parse

from .client import MaterialsCloudClient, new_session
from .discover import discover
from .fetching import fetch_with_retries
from .triage import triage


def run(record_ids: list[str], work: Path, keep: bool = False) -> int:
    man, raw, ds = work / "manifests", work / "raw", work / "dataset"
    checks: list[tuple[str, bool, str]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append((name, bool(ok), detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' — ' + detail) if detail else ''}",
              flush=True)

    sent_auth: list[str] = []

    def _session():  # an anonymous MC session that records any Authorization header it sends
        s = new_session()
        orig = s.request

        def _req(method, url, *a, **k):  # type: ignore[no-untyped-def]
            hdrs = {**s.headers, **(k.get("headers") or {})}
            if any(h.lower() == "authorization" for h in hdrs):
                sent_auth.append(url)
            return orig(method, url, *a, **k)
        s.request = _req  # type: ignore[method-assign]
        return s

    try:
        print(f"== Materials Cloud smoke test in {work} on {record_ids}", flush=True)
        d = discover(MaterialsCloudClient(), man / "mc_candidates.jsonl",
                     only_ids=record_ids, rejections_path=man / "mc_rejections.jsonl")
        check("discover: candidates written", d["candidates"] > 0,
              f"{d['candidates']} candidate(s), dropped {d['dropped_by_reason']}")
        t = triage(man / "mc_candidates.jsonl", man / "mc_keep.jsonl", session=_session(),
                   rejections_path=man / "mc_rejections.jsonl", interval=0.0)
        check("triage: kept fetch unit(s)", t["fetch_units"] > 0,
              f"decisions {t['decisions']}, {t['fetch_units']} unit(s), "
              f"{t['bytes_to_fetch'] / 1e6:.1f} MB to fetch, peeked {t['peeked']}")
        if t["fetch_units"] == 0:
            return _finish(checks, work, keep)
        f = fetch_with_retries(
            lambda: shared_fetch(man / "mc_keep.jsonl", out_path=man / "mc_fetched.jsonl",
                                 raw_dir=raw, rejections_path=man / "mc_fetch_rejections.jsonl",
                                 max_bytes=None, workers=2, session_factory=_session),
            man / "mc_keep.jsonl", man / "mc_fetched.jsonl", man / "mc_fetch_rejections.jsonl",
            retries=8)
        fetched_rows = list(read_jsonl(man / "mc_fetched.jsonl")) \
            if (man / "mc_fetched.jsonl").is_file() else []
        check("fetch: calc units staged", sum(r["n_calc_units"] for r in fetched_rows) > 0,
              f"{len(fetched_rows)} unit(s), {sum(r['n_calc_units'] for r in fetched_rows)} calc "
              f"unit(s) in {f.get('fetch_passes')} pass(es); still pending: "
              f"{f.get('pending_after_retries') or 'none'}")
        check("fetch: no Authorization header ever sent", not sent_auth,
              f"{len(sent_auth)} request(s) carried one" if sent_auth else "anonymous")
        prov = fetched_rows[0]["provenance"] if fetched_rows else {}
        check("fetch: provenance is Materials Cloud",
              prov.get("source") == "materials_cloud" and bool(prov.get("doi")),
              f"source={prov.get('source')} record_id={prov.get('record_id')} doi={prov.get('doi')}")
        pstats = parse(man / "mc_fetched.jsonl", dataset_dir=ds, raw_dir=raw,
                       rejections_path=ds / "rejections.jsonl", parse_timeout_s=600)
        check("parse: frames written", pstats["frames"] > 0,
              f"{pstats['calcs_parsed']} calc(s), {pstats['frames']} frame(s), "
              f"{pstats['rejections']} rejection(s)")
        metas = list(read_jsonl(ds / "metadata.jsonl")) if (ds / "metadata.jsonl").is_file() else []
        check("parse: calc_ids namespaced materials_cloud:",
              bool(metas) and all(m["calc_id"].startswith("materials_cloud:") for m in metas),
              metas[0]["calc_id"] if metas else "")
        # some VASP 5.x OUTCARs echo an EMPTY "INCAR:" block, so the resolved `parameters` block
        # (read from the header by outcar_params) counts as the calc-parameter record too
        cps = [m.get("calc_parameters") or {} for m in metas]
        check("parse: calc_parameters recovered (INCAR/parameters + POTCAR + run_type)",
              bool(cps) and all((cp.get("incar") or cp.get("parameters")) and cp.get("potcar_spec")
                                and cp.get("run_type") for cp in cps),
              f"parsers: {sorted({str(m.get('parser')) for m in metas})}, run_types: "
              f"{sorted({str(cp.get('run_type')) for cp in cps})}")
        v = verify_dataset(ds)
        check("verify: metadata<->shard bijection", v.get("ok", False),
              f"{(v.get('integrity') or {}).get('n_frames_metadata', '?')} frames")
        print(json.dumps({"discover": d, "triage": t, "fetch": f, "parse": pstats},
                         indent=1, default=str)[:4000])
    except Exception as exc:  # noqa: BLE001 - report the failure as a FAIL line
        check("smoke run completed without exception", False, f"{type(exc).__name__}: {exc}")
    return _finish(checks, work, keep)


def _finish(checks: list[tuple[str, bool, str]], work: Path, keep: bool) -> int:
    n_fail = sum(1 for _, ok, _ in checks if not ok)
    print(f"== {len(checks) - n_fail}/{len(checks)} checks passed "
          f"({'PASS' if n_fail == 0 and checks else 'FAIL'})")
    if keep:
        print(f"work dir kept: {work}")
    else:
        shutil.rmtree(work, ignore_errors=True)
    return 0 if n_fail == 0 and checks else 1
