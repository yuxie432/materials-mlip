# CSD3 smoke test — validate the whole harvest on real Zenodo data before the big run

A small, real, end-to-end shake-out of every stage (discover → triage → fetch → parse →
store), both parallelism models (overlapped `pipeline` **and** the `split`/array/`merge`
flow), **every safety valve** (disk bytes, disk inodes, parse RAM), and **resumability at
every stage** — sized to finish in minutes on a handful of small records, and writing to a
**separate** data dir (`…/hpc-work/zenodo_smoketest`) so it can never touch a real harvest.

Each script prints a clear result and (t25) exits non-zero on failure, so `sacct … ExitCode`
is the verdict. This whole suite was dry-run end-to-end off-cluster first; on CSD3 the one
unknown it proves is **compute-node internet + the /rds quota behaviour under the valves.**

```
00_check_network.sh          # (existing) prove icelake + icelake-himem reach zenodo.org  <-- RUN FIRST
t10_smoke_discover.sh        # small real discover (--max-records) + triage -> tiny keep.jsonl
t20_smoke_pipeline.sh        # REAL `pipeline` happy path + status + frame/metadata inspection
t25_smoke_safety.sh          # ALL safety valves + resumability (adaptive limits; exit code = verdict)
t30_smoke_array.sh           # split -> per-task parse -> merge -> verify -> purge (+ merge idempotency)
make_test_keeplist.py        # helper: pick the N smallest confirmed records from a triaged keep-list
inspect_dataset.py           # helper: frame-label fidelity + metadata + verify (a PASS/FAIL gate)
smoke_safety.py              # helper: the in-process safety/resume harness t25 runs
```

## 0. One-time setup on CSD3

```bash
# code on /home (backed up); data on /rds scratch.
git clone <this repo> ~/materials-mlip && cd ~/materials-mlip
module load python/3.11.0-icl
python -m venv ~/materials-mlip/.venv && source ~/materials-mlip/.venv/bin/activate
pip install -e '.[parse,archives]'          # parse = pymatgen+ase; archives = py7zr/rarfile/zstandard
#   `.7z` (py7zr) and `.tar.zst` (zstandard) work immediately — pure-Python, no system binary.
#   `.rar` is different: `rarfile` is only a WRAPPER that shells out to an external
#   unrar/unar/bsdtar binary, which pip cannot install and CSD3 does not ship on PATH. `.rar` is
#   rare in Zenodo VASP data, so skipping it is fine — those records log a NON-FATAL
#   `archive_unsupported` and can be re-collected later with `fetch --retry-rejected`. To add rar
#   support (your ~/bin is already on PATH):
#       conda create -y -p ~/arctools -c conda-forge unar libarchive
#       ln -s ~/arctools/bin/unar ~/arctools/bin/bsdtar ~/bin/ && hash -r && which unar bsdtar
printf 'ZENODO_TOKEN=<your token>\n' > .env  # gitignored; raises the file-endpoint request quota
mkdir -p logs                                # SLURM opens -o/-e BEFORE the job body runs
```

The account is **not** hardcoded in the scripts. Set it via `export
SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU` (find it with `mybalance`) — `sbatch` reads that as
`--account`, and it propagates to `20_pipeline.sh`'s RESUBMIT chain. Put it in `~/.bashrc`
(untracked) to persist, or export it each session (shown below). Keeping it in the environment
rather than a tracked `#SBATCH -A` line means the account never diverges between your local and
CSD3 clones and never lands in git.

Every smoke-test job carries `#SBATCH --mail-type=END,FAIL`, so you get an email as each one
ends or fails. The address defaults to your CSD3 address; override it (kept out of git, like
the account) with `export SBATCH_MAIL_USER=you@example.com` before submitting.

## 1. Prove compute nodes can reach Zenodo (the one real unknown)

```bash
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU                         # your account (mybalance)
bash scripts/csd3/00_check_network.sh                          # ~1-2 min; probes icelake + himem
```
PASS → continue. FAIL → the fetch/discover stages can't run on compute nodes; see the
"Do compute nodes have outbound internet?" section of `scripts/csd3/README.md` (proxy / login-node
fallback). **Everything below needs this to pass.**

## 2. Run the smoke test (activate the env first; it propagates via `--export=ALL`)

```bash
module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
export SBATCH_ACCOUNT=<MYGROUP>-SL3-CPU                                # your account (mybalance)
export SBATCH_MAIL_USER=you@example.com                               # OPTIONAL: END/FAIL emails (else default addr)
export ZENODO_HARVEST_DATA=/rds/user/$USER/hpc-work/zenodo_smoketest   # SEPARATE from any real harvest
mkdir -p logs

T10=$(sbatch --parsable scripts/csd3/test/t10_smoke_discover.sh)       # discover + triage -> keep.jsonl
sbatch --dependency=afterok:$T10 scripts/csd3/test/t20_smoke_pipeline.sh   # happy-path pipeline + inspect
sbatch --dependency=afterok:$T10 scripts/csd3/test/t25_smoke_safety.sh     # safety valves + resume
sbatch --dependency=afterok:$T10 scripts/csd3/test/t30_smoke_array.sh      # array/merge/verify/purge
```
`--dependency=afterok:$T10` lets you queue all four at once; t20/t25/t30 wait for the keep-list.
They use independent sub-dirs, so t20/t25/t30 can run concurrently.

Watch them:
```bash
squeue -u $USER
tail -f logs/zh-t20-pipeline-*.out          # .err for tracebacks
sacct -j <jobid> --format=JobID,JobName,State,Elapsed,MaxRSS,ExitCode   # MaxRSS = peak parse RAM
```

## 3. What PASS looks like

| Job | Proves | PASS criteria (in the `.out` log) |
|---|---|---|
| **t10** | discover, triage (real zip-peek), dedup, license/access gates | `keep.jsonl has N records` (N = your `COUNT`, default 8) |
| **t20** | fetch (incl. targeted zip fetch), parse (pymatgen **and** ASE OUTCAR), store, purge, `pipeline` overlap, `status` | pipeline summary `"ok": true`; `status` shows PARSE ~100%; `=== INSPECT PASS ===` |
| **t25** | disk-bytes valve, disk-inode valve, RAM guard, fetch resume, parse resume | the PASS/FAIL table ends `OVERALL: PASS`; job **ExitCode 0** |
| **t30** | split, per-task parse, `merge-datasets` (+ resume idempotency), `verify`, `purge-raw` | `merge ok=True`; `verify ok=True` with `meta==disk`; `raw files remaining: 0`; `INSPECT PASS` |

`inspect_dataset.py` (run inside t20/t30) is the "check the frames + metadata" gate: it asserts
every frame carries `REF_energy`/`REF_forces`, that `REF_stress`/`E_free` are present where
produced, that a convergence-**unknown** calc never reads back `electronic_converged=True`
(the round-trip regression), and prints a sample frame + a structured metadata record
(provenance, calc parameters, quality, availability).

## 4. Resumability — the wallclock-kill drill (optional, manual)

t25 already proves fetch/parse resume and the disk-valve pace-and-resume loop
non-interactively. To also see a job survive a hard SLURM kill exactly as the real 12 h/36 h
harvest will:

```bash
# submit the REAL production pipeline against the smoke keep-list, then kill it mid-run
# (SBATCH_ACCOUNT already exported above):
J=$(sbatch --parsable scripts/csd3/20_pipeline.sh)   # reads $ZENODO_HARVEST_DATA/manifests/keep.jsonl
sleep 45 && scancel $J                                # simulate a wallclock SIGKILL
sbatch scripts/csd3/20_pipeline.sh                    # resume: skips done recids, prunes orphans, continues
# the resumed run's log shows `skipped_existing` on fetch and `parse resume: N calc(s) already done`.
```
For the real harvest, `RESUBMIT=1 sbatch scripts/csd3/20_pipeline.sh` chains follow-on jobs
automatically across wallclock kills (do **not** use RESUBMIT for the smoke test).

## 5. Two operational subtleties this test surfaced (not bugs — worth knowing)

1. **`pipeline`'s split parts dir is keyed off `--in`, not `--raw-dir`.** Its `*.fetched.jsonl`
   sidecars live in `<in>.pipeline_parts/` (or `--parts-dir`). Re-running with the **same**
   `--in` = resume (intended). But pointing the same `--in` at a **different** `--raw-dir`
   reuses the old fetched manifests and parse then fails `FileNotFoundError`. The test scripts
   pass an explicit `--parts-dir` to stay isolated; in production you only ever resume the one
   run, so this doesn't arise.
2. **Size the disk valve above the largest single record's *transient* peak** (extracted files
   **+** the archive still on disk mid-extraction), not just its final size. A record whose
   transient peak exceeds the budget is *truncated* (kept partial, logged
   `record_exceeds_disk_budget`, non-terminal) — and because it's written to the fetched
   manifest, a plain resume with the same budget won't re-collect it (raise the budget AND drop
   the recid from that manifest). At the production 800 GB budget no single Zenodo record comes
   close, so this only matters at the tight limits used for valve testing.

## 6. Clean up the test data when done

```bash
rm -rf /rds/user/$USER/hpc-work/zenodo_smoketest    # all smoke-test data (manifests/raw/dataset/work)
rm -f  logs/zh-t*-*.out logs/zh-t*-*.err            # smoke-test job logs
```
The smoke test never writes to a production `ZENODO_HARVEST_DATA`, so the real harvest's data
is untouched.
