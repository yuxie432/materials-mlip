# Running the Materials Cloud harvest on CSD3

Batch templates, probe and benchmark for harvesting VASP data from the **Materials Cloud
Archive**. They mirror the NOMAD templates (`../nomad/`) and share their conventions — set the
account once with `export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU`, activate the env before `sbatch`,
`mkdir -p logs_mc` before the first submit. Design + live-verified API facts:
`docs/MATERIALS_CLOUD_HARVEST.md`.

**Status: harvest RUN (2026-09-24/25, jobs `36245037` + `36251084`) — 102 records / 75,751 calcs /
2.55M frames, verify exact; result: `docs/MATERIALS_CLOUD_HARVEST_RESULT.md`.** Left to do: step 4
(the overlap check) and, when no re-parse is planned, `rm -rf $MC_HARVEST_DATA/raw` (3.9 GiB /
13.8k inodes of rejected calcs' files). Step 5 is not needed (no deferred primaries).

```
scripts/csd3/materials_cloud/10_discover.sh     # stage 0-1: full census + gates + overlap flags + peeks
scripts/csd3/materials_cloud/15_bench.sh        # sizing: speed probe + AiiDA census + fetch/parse pilot (RUN 2026-09-24)
scripts/csd3/materials_cloud/20_pipeline.sh     # stage 2-4 overlapped (fetch || parse+purge) + verify
scripts/csd3/materials_cloud/30_bigparse.sh     # OPTIONAL phase B: primaries above the pipeline's cap (~3.2 GB)
scripts/csd3/materials_cloud/csd3_mc_overlap.py # after the pipeline: are the Zenodo-flagged records duplicates?
scripts/csd3/materials_cloud/csd3_mc_probe.py   # --speed / --aiida probes (run by 15_bench.sh)
scripts/csd3/materials_cloud/csd3_mc_bench.py   # real-data fetch+parse pilot (run by 15_bench.sh)
```

## Separate MC tree (never mixed with Zenodo / NOMAD)

Everything Materials Cloud writes lives under **its own sibling root** `$MC_HARVEST_DATA`
(default `/rds/user/$USER/hpc-work/materials_cloud`). The Zenodo and NOMAD datasets are only
*read* (their `dataset/metadata.jsonl`) for the overlap flags at discover time.

```
/rds/user/$USER/hpc-work/
├── zenodo/            (Zenodo harvest)          <- read-only here
├── nomad/             (NOMAD harvest)           <- read-only here
└── materials_cloud/   manifests/ raw/ dataset/  <- this harvest
logs_mc/               SLURM .out/.err + per-job summary JSON (repo-relative, gitignored)
```

## Order of operations (run these on CSD3, from the repo root)

```bash
cd ~/materials-mlip && git pull
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo
export NOMAD_HARVEST_DATA=/rds/user/$USER/hpc-work/nomad
export MC_HARVEST_DATA=/rds/user/$USER/hpc-work/materials_cloud
mkdir -p logs_mc
MAN=$MC_HARVEST_DATA/manifests

# 0. (~1 min) live end-to-end smoke on a 16.5 MB record, isolated temp dir:
python -m materials_cloud_harvest.cli smoke          # expect "9/9 checks passed (PASS)"

# 1. census + triage under the v3 rules (ZIP64 fix, sqlite AiiDA databases, --unresolved all).
#    Cached v2 peeks are re-done (~10 min + ~10 min of AiiDA databases); resumable.
DISC=$(sbatch --parsable scripts/csd3/materials_cloud/10_discover.sh)

# 2. the harvest, queued behind 1 (defaults sized from the 2026-09-24 bench for a DEDICATED
#    ~880 GB / ~990k-inode slice: 8 icelake-himem cpus, 6 fetch + 4 parse workers, parse memory
#    budget ~37 GiB, primary cap ~3.2 GB, valve 780 GB / 900k inodes, 24 parts — the budget and cap
#    follow the allocation, e.g. `sbatch -c 12 …` or `sbatch -p icelake -c 16 …`):
RESUBMIT=1 sbatch --dependency=afterok:$DISC scripts/csd3/materials_cloud/20_pipeline.sh
#    before it starts you can sanity-check the new keep-list (expect ~1.2k units / ~1.2 TB):
python -c "import json;print(json.dumps(json.load(open('$MAN/mc_keep.report.json'))['summary'],indent=1))"

# 3. watch / check (read-only, safe while it runs)
python -m materials_cloud_harvest.cli status --max-disk-bytes 780000000000 --max-disk-files 900000
tail -f logs_mc/mc-pipeline-*.err
python -m zenodo_harvest.cli verify --dataset-dir $MC_HARVEST_DATA/dataset

# 4. after the pipeline: are the two Zenodo-flagged records calc-level duplicates? (login node OK)
python scripts/csd3/materials_cloud/csd3_mc_overlap.py --mc-root $MC_HARVEST_DATA \
    --zenodo-dataset $ZENODO_HARVEST_DATA/dataset        # -> $MAN/mc_overlap.json

# 5. ONLY if the pipeline deferred primaries above its ~3.2 GB cap (status shows primary_too_large):
sbatch scripts/csd3/materials_cloud/30_bigparse.sh        # 32 cpus, one parse at a time, 16 GB cap
```

## What bounds the harvest, and the knobs (measured on CSD3, 2026-09-24)

| stage | what bounds it | measured | knob (20_pipeline.sh / 10_discover.sh) |
|---|---|---|---|
| triage peeks (~1.4k + ~130 AiiDA databases) | **request latency** — each is 1-3 small Range reads through MC's API 302 hop | 64 KiB Range read 0.09 s, redirect 0.04 s; 1,434 peeks in 10 min | `PEEK_WORKERS=4` (paced 0.4 s apart: well under 500 req/60 s) |
| fetch (~1.2k units / ~1.2 TB, ~1.1 TB blind) | **S3 bandwidth** | 52 MB/s on 1 stream, 57 on 4, **96 on 8** | `WORKERS=6` → ≈ 4-5 h (8 with more cores) |
| many small calcs (~10⁴-10⁵) | **parse throughput** — one forkserver child per calc | 0.19 s/calc serial, ×3.8 with 4 workers | `PARSE_WORKERS=6` |
| multi-GB primaries | **RAM** — pymatgen peaks ~4-12× the uncompressed file | pilot max 1.04 GB → 3.8 GiB peak; the 2026-09-24 run: none over 2.5 GB | `PARSE_MEM_BUDGET` (= job RAM − 16 GiB) + `MAX_PRIMARY_BYTES` (= budget/12 ≈ 3.2 GB at 8 himem cores) |

The **parse memory budget** is what lets 4 workers and a ~3.2 GB cap coexist on 8 cores: every parse
first reserves ~12 × its primary (+0.5 GiB) from the budget, FIFO — small calcs run 6-way, a big
AIMD vasprun waits for room and then runs alone. Without it the rule would be
`workers × 12 × cap ≤ RAM` (3 workers → 2.5 GB cap on 16 cores, as first planned). All four knobs
are env overrides (`WORKERS=… PARSE_WORKERS=… sbatch …`); the budget and cap follow
`--cpus-per-task` automatically (`RSS_RATIO` must be an integer).

A token is **not** needed: every record is public, the API limit is not approached, and the bytes
come from presigned S3 URLs a token would not speed up.

## What to look at in the triage report (`mc_keep.report.json`)

* `summary.kept_records` / `fetch_units` / `bytes_to_fetch` — the harvest size.
* `summary.decisions` — `vasp_mention` (kept fail-safe), `vasp_evidence` (**blind spot recovered**:
  records that never mention VASP but whose zips/AiiDA archives hold VASP outputs),
  `unresolved_fetch` (**blind-fetched**: archives no peek can settle), `peek_proved_no_vasp`,
  `no_vasp_evidence`, `evidence_gap_only`.
* `summary.blind_spot_recovered` / `summary.blind_fetch` — the recovered records / the blind-fetch
  size (records, units, bytes). After the pipeline, the fetch rejections say which blind units held
  no VASP (`no_vasp_files_fetched`).
* `summary.unpeekable_skipped` — only non-zero under `UNRESOLVED=dft|none`.
* per-record `gaps` — unreadable archives, nested archives, databases not inspected.

And in the discover log: `flagged_overlap` + `overlap_examples` — MC records linked to (or titled
like) harvested Zenodo records, or cited by NOMAD calcs. They are **kept** (all MC→Zenodo links are
`IsSupplementTo`); the flags travel in provenance for the training-time dedup.

## Sizing (dedicated slice: 115 GB / 6.6k files used of 1 TB / 1M on 2026-09-24)

* **Disk**: the valve bounds only `$MC_HARVEST_DATA/raw`: `MAX_DISK_BYTES=780e9`,
  `MAX_DISK_FILES=900000` (~88% / ~91% of the free space; the MC dataset + manifests — tens of GB,
  a few thousand files — are not valve-tracked). Multi-archive records are split per archive, and
  blind-fetched archives are deleted right after extraction, so the peak is the in-flight archives
  (≤ 8 at once, the largest 44 GB) plus two parts' extracted VASP files. Lower both if other jobs
  share the quota again.
* **RAM**: 8 `icelake-himem` cores = ~53 GiB: ~37 GiB parse memory budget + 16 GiB for the main
  process and the 6 fetch workers (a blind-fetched zip with ~10M members holds ~7 GB of zipfile
  directory). The ~3.2 GB cap is above every primary seen so far (the 2026-09-24 run parsed the
  whole evidenced set with none over 2.5 GB; the bench's largest was 1.04 GB); anything bigger stays
  staged for `30_bigparse.sh` (32 cores, 16 GB). Why not 20 cores: that bought a ~10 GB cap nobody
  needs and a 20-core × 12 h block that waits hours on the small himem partition.
* **Time**: fetch ≈ 3.5 h at 96 MB/s, parse overlapped → one 12 h SL3 job expected; `RESUBMIT=1`
  covers an overrun and retries any unit still failing transiently (the pipeline exits non-zero).
* **Archives**: `.7z`/`.rar` need the `archives` extra and an `unrar` (the script prepends `~/bin`,
  where the static RARLAB one lives, and prints a warning at start if a backend is missing).

## Rate limits & etiquette

MC allows 500 requests / 60 s (`X-RateLimit-Limit`, `Retry-After: 60` on 429 — honoured). Each file
request is a 302 from the API (latency, counted) + a presigned CSCS S3 transfer (bandwidth, not
counted). No token is used or needed — the adapter's anonymous session guarantees the Zenodo token
is never sent, and the shared fetch only ever attaches it to `zenodo.org`.
