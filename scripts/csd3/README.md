# Running the harvest on CSD3

Batch templates for the full Zenodo harvest on Cambridge's CSD3. The account is **not**
hardcoded in the scripts — set it once per session/machine via `export
SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU` (find yours with `mybalance`), which `sbatch` reads as
`--account`. Keeping it in the environment (not in a tracked `#SBATCH -A` line) means the
account never diverges between your local and CSD3 clones and never lands in git. Then edit
the ENV SETUP block if needed and submit from the repo root.

```
scripts/csd3/00_check_network.sh   # ONE-OFF: prove icelake AND icelake-himem nodes reach zenodo.org
scripts/csd3/csd3_download_speed.py# ONE-OFF: measure compute-node download throughput (fetch-time estimate)
scripts/csd3/10_discover.sh        # stage 0-1 (discover + triage) — network, 1 core
scripts/csd3/20_pipeline.sh        # stage 2-4 overlapped (fetch || parse+purge) + verify
scripts/csd3/30_parse_array.sh     # OPTIONAL: many-core array parse of a fetched manifest
scripts/csd3/31_merge_verify.sh    #   ...then merge the per-task dirs + verify + purge
```

## CSD3 facts these scripts are built around

| Fact | Value | Consequence for the harvest |
|---|---|---|
| Wallclock limit | **36 h** (SL1/SL2), **12 h** (SL3) | The harvest can exceed one job → every stage is resumable; `20_pipeline.sh` can self-resubmit (`RESUBMIT=1`). Longer runs need the `-long` QoS (up to 7 days) which must be requested from `support@hpc.cam.ac.uk` and uses the `icelake-long`/`cclake-long` partitions. |
| `/rds/user/$USER/hpc-work` | **1 TB and 1 million files**, Lustre, **not backed up** | Point `ZENODO_HARVEST_DATA` here. `--max-disk-bytes ~0.8 TB` **and** `--max-disk-files 800000`: measured, the inode limit binds FIRST (extracted VASP trees are ~7.6 KiB median per file, so 1M files ≈ 0.3 TB). |
| `/home/$USER` | 50 GB, backed up | Code and `.env` only — never job I/O. |
| Login nodes | capped at ~4 CPUs per user | Fine for a smoke test, **not** for the real fetch — run it as a batch job. |
| Array jobs | `--array=0-N`, `$SLURM_ARRAY_TASK_ID` | Used by `30_parse_array.sh` (parse is CPU-bound and embarrassingly parallel). |

Sources: [Quick Start](https://docs.hpc.cam.ac.uk/hpc/user-guide/quickstart.html),
[I/O management](https://docs.hpc.cam.ac.uk/hpc/user-guide/io_management.html),
[long jobs](https://docs.hpc.cam.ac.uk/hpc/user-guide/long.html).

## Do compute nodes have outbound internet?

**Verified YES on 2026-07-31** — `scripts/csd3/00_check_network.sh` PASSed on both
`icelake` and `icelake-himem`, so the fetch stage runs in batch as designed and no proxy /
login-node fallback is needed. The CSD3 docs still don't state it, so the probe below is
kept for re-verifying if the account or site network policy ever changes. To re-run it
(a ~1-2 minute interactive job), activate the harvest env first (so `requests` imports and
`srun` propagates it):

```bash
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
ACCOUNT=MYGROUP-SL3-CPU bash scripts/csd3/00_check_network.sh   # probes icelake AND icelake-himem
```

It probes **both** node types the harvest uses — `icelake` (discover + triage) and
`icelake-himem` (where `20_pipeline.sh` runs the fetch) — because outbound access could in
principle differ between them; restrict with e.g. `PARTITION=icelake`. A login-node
pre-flight fails fast with an actionable message if `requests` is missing, so a
not-activated env never masquerades as a firewall. The harvest's fetch/discover/triage need
outbound HTTPS to `zenodo.org`. If compute nodes turn out to be firewalled:

* check whether a proxy is published for your project (then export `https_proxy` in the
  ENV SETUP block — `requests` honours it automatically); or
* ask `support@hpc.cam.ac.uk`; or
* fall back to running only `fetch` on a login node with `--workers 2` and a modest
  `--max-disk-bytes`, in `screen`/`tmux`, and keep `parse`/`merge`/`verify` in batch jobs
  (they need no network). Fetch is fully resumable, so it can be stopped and restarted.

## Order of operations

```bash
# Activate the harvest env FIRST. sbatch/srun capture your submit environment by default
# (--export=ALL) and carry it to the compute node — including through 20_pipeline.sh's
# RESUBMIT chain — so this one activation covers every job below. (The scripts deliberately
# do NOT `module load` themselves: a failed load would abort the job under `set -e`.)
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate

export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU                       # your account (mybalance); sbatch's --account
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo   # scratch, read at import
mkdir -p logs                                                # SLURM opens -o/-e before the job runs
DISC=$(sbatch --parsable scripts/csd3/10_discover.sh)        # ~1-2 h (rate-limited)
sbatch --dependency=afterok:$DISC scripts/csd3/20_pipeline.sh   # starts only if discover succeeds
# optional many-core parse instead of the pipeline's serial parse:
sbatch scripts/csd3/30_parse_array.sh
```

`--dependency=afterok:$DISC` lets you queue both at once — the pipeline sits PENDING until
discover finishes cleanly. Use `sbatch` (not `bash`): the `#SBATCH` directives only take
effect under `sbatch`, so `bash 20_pipeline.sh` would run the whole pipeline on the login
node (capped ~4 CPUs, wrong partition). For the self-resubmitting long run add `RESUBMIT=1`.

`mkdir -p logs` is required **before the first submit**: every script's `#SBATCH -o logs/…`
is opened by SLURM before the script body (which does its own `mkdir`) runs, so a missing
`logs/` makes the job fail to start.

**`$TMPDIR` (node-local, not `/rds`, not `/tmp`).** `parse` copies each OUTCAR into a
tempdir before ASE reads it, so `20_pipeline.sh` / `30_parse_array.sh` point `TMPDIR` at
`/local` (the node's local disk, 57–131 GB, auto-removed at job end). This keeps a large
temp copy off a possibly-tmpfs `/tmp` (which would silently eat the RAM budget that
`--max-primary-bytes` is calibrated against) and off the `/rds` quota (the disk valve does
not track `$TMPDIR`). When running the calibration helpers by hand, prepend `TMPDIR=/local`
to their `srun` too (they also write a few hundred MB – ~1.6 GB of transient files).

## Monitoring a running job

**SLURM job state** (queueing/running, and — importantly — peak RAM vs the parse budget):

```bash
squeue -u $USER                                                      # PENDING/RUNNING + why
sacct -j <jobid> --format=JobID,JobName,State,Elapsed,MaxRSS,ExitCode  # incl. peak RSS
tail -f logs/zh-pipeline-<jobid>.out   # live stdout   (.err for tracebacks)
```

**Harvest progress** — the `status` subcommand is a read-only snapshot (no network, no
lock: safe to run *while* the job writes these files). It line-counts the append-only
manifests and walks the raw/dataset trees:

```bash
python -m zenodo_harvest.cli status \
    --max-disk-bytes 800000000000 --max-disk-files 800000    # to show staging as % of quota
# refresh every minute:
watch -n 60 'python -m zenodo_harvest.cli status --max-disk-bytes 800000000000 --max-disk-files 800000'
```

```
DISCOVER  candidates: 18,742
TRIAGE    keep-list:   6,120
FETCH     fetched:     1,504 / 6,120  (24.6%)   calc_units: 3,880
PARSE     parsed:      3,120 / 3,880 calc_units  (80.4%)   frames: 214,553
STORE     shards: 22   dataset: 3.1 GiB
STAGING   raw: 512.0 GiB / 745.1 GiB (68.7%)   inodes: 410,233 (files … + dirs …) / 800,000 (51.3%)
ERRORS    rejections: 47  →  archive_no_vasp 31, primary_too_large 9, member_too_large 7
```

Pass the **same** `--max-disk-bytes`/`--max-disk-files` you gave the pipeline so the staging
line reads as a % of your real quota. `--json` gives machine output; `--dataset-dir` /
`--raw-dir` / `--manifests-dir` / `--keep` override the (env-derived) defaults. `status`
is read-only and always exits 0. It reports **dropped** records by reason (`rejections`);
a *hard* stage failure (e.g. a background parse crash) shows in the pipeline's
`logs/…summary.json` and the `.err` log, not here. The staging walk touches every inode
under `raw/`, so keep the `watch` interval modest (≥30 s) during the uncapped run.

## Parse memory (icelake-himem)

`parse` (pymatgen) is the only memory-hungry stage — its peak RSS is **~10× the
vasprun.xml/OUTCAR file size** (measured on CSD3 icelake-himem: a 534 MB file peaks at
~5.6 GB), and an over-budget parse is a cgroup **SIGKILL** of the whole job, not a catchable
error. On CSD3 memory is bundled with cores (icelake-himem = 6760 MiB/core), so
`--cpus-per-task` on the parse/pipeline jobs is bought for **RAM, not compute** (pymatgen is
single-threaded). Size `--max-primary-bytes` to the job's RAM: `~0.85 × cpus-per-task ×
6.76 GB ÷ 10`, leaving room for the concurrent fetch (`--cpus-per-task=4` → ~26 GiB →
~2.0 GB). Over-cap primaries are skipped (`primary_too_large`) and kept on disk for a later
bigger-RAM re-parse. Synthetic samples read a touch high, so confirm the ratio on your own
data/hardware:

```bash
TMPDIR=/local srun -A MYACCT-SL3-CPU -p icelake-himem --cpus-per-task=4 --time=00:20:00 \
    python scripts/csd3/csd3_parse_memory.py --raw-dir $ZENODO_HARVEST_DATA/raw --top 8
# before any fetch, calibrate on synthetic samples instead:
TMPDIR=/local srun -A MYACCT-SL3-CPU -p icelake-himem --cpus-per-task=4 --time=00:20:00 \
    python scripts/csd3/csd3_parse_memory.py --synthetic
```

Discovery is hard-limited by Zenodo to 30 search requests/minute, so it is single-stream
by design — do **not** parallelise it. Measured 2026-07-29 for the 3-resource-type run
(current keywords): ~18.7k hits ≈ 750 pages ≈ **~51 min** of paging. Only `fetch` benefits
from `--workers`.

## Download throughput (the fetch-time estimate)

`fetch` dominates the harvest wall-clock, and the one unknown is the **compute-node →
zenodo.org throughput** (the CSD3 docs don't state it, and it depends on the node's outbound
path / any proxy). Measure it directly on a compute node **before** sizing the job:

```bash
TMPDIR=/local srun -A MYACCT-SL3-CPU -p icelake --nodes=1 --ntasks=1 --cpus-per-task=4 --time=00:20:00 \
    python scripts/csd3/csd3_download_speed.py --workers 4
```

It reports single-stream MB/s, N-worker aggregate MB/s (the number to divide the transfer
by), and Range round-trip latency (the peek / targeted-zip-fetch cost). Plug the aggregate
into: **fetch time ≈ transfer ÷ aggregate MB/s**.

**Measured on CSD3 (icelake compute node, 2026-07-29, anonymous):** `--workers 4` →
single-stream 28.1 MB/s, **aggregate 66.5 MB/s**; `--workers 8` → 7.5 / 47.3 MB/s (no gain,
and depressed because the 4 default URLs get double-fetched — pass ≥N distinct `--url`s to
test N>4 cleanly). Two takeaways: throughput **does not scale past ~4 workers** (node egress /
Zenodo per-IP shaping caps ~50–66 MB/s), and it is **variable run-to-run** — plan conservatively
at **~50 MB/s aggregate**. Range latency 73–85 ms ⇒ peek/walk are quota-bound (~100 req/min),
not latency-bound.

Against the measured 2026-07-29 transfer (see `docs/survey-findings.md`) — **~7.5 TB uncapped**
(peek + zip-walk-aware) or **~1.1 TB at `--max-bytes 2e9`** — at ~50–66 MB/s that is:

| policy | fetch time | + discover+peek | total | jobs |
|---|--:|--:|--:|---|
| uncapped ~7.5 TB | ~33–46 h | +1.5 h | **~35–48 h** | **2** (self-resubmit) or `-long` QoS |
| `--max-bytes 2e9` ~1.1 TB | ~5–6 h | +1.5 h | **~6–8 h** | **1** (fits 12 h SL3) |

**Fetch bandwidth is the rate-limiting stage.** Discover (~51 min, search 30/min) and
triage-peek (~1 h; request-rate-bound — a full peek of ~2,200 zips absorbed 225 × HTTP 429)
are small fixed costs, and parse overlaps fetch in `pipeline` (47 frames/s/core → free). **Set
`ZENODO_TOKEN`** for the real run: it won't raise the ~66 MB/s bandwidth ceiling but lifts the
file-endpoint request quota, cutting 429 stalls on peek + small-file fetches. Use `--workers 4`
(8 gives no benefit).

## Resuming

Everything is resumable; re-running the same command continues where it stopped.

* `discover` — appends to `<out>.hits.jsonl` and skips completed query/date windows.
* `triage` — **not** resumable (it rewrites its keep-list), but cheap: only `archive`
  records are peeked, ~600 of them for the measured candidate set (~10 min). If it is
  interrupted, just re-run it.
* `fetch` — skips recids already in `fetched.jsonl`, and **resumes a part-transferred
  file over HTTP Range** (a killed job does not restart a 100 GB archive from byte 0).
* `parse` — skips calc_ids already in `metadata.jsonl`, prunes orphan frames from a
  crashed writer.
* `pipeline` — re-splits the same parts, so each batch's fetch/parse skip what is done.

## Staying inside the quota

`--max-disk-bytes` / `--max-disk-files` are enforced on **actual** usage (every byte and
file is charged as it is written), not on any predicted decompression ratio — measured
expansion on real records spans ~1x to 4.1x. When a limit is reached the record being staged
is rolled back whole, fetch stops resumably, and `pipeline` runs `parse` + `purge-raw` to
reclaim the space before re-fetching the same batch. Each run reports its own
`peak_staged_bytes` / `peak_staged_files`, so check those in the job log against your limits.

If the volume fills anyway, the `EDQUOT` write errors are treated as transient and those
records are retried by a later run — they are not silently dropped.

One caveat: if a **killed job leaves a `.parse.lock`** in the dataset dir, a lock from a
*different host* is never assumed stale (cross-node liveness is uncheckable). Confirm no
parse is running, then `rm <dataset-dir>/.parse.lock`.
