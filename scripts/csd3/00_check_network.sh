#!/bin/bash
# ONE-OFF precheck: can a CSD3 COMPUTE node reach the Zenodo API?
#
# The CSD3 docs do not state whether compute nodes have outbound internet access, and the
# whole fetch stage depends on it. Run this (interactive, ~1-2 min, a couple of core-minutes
# of your allocation) BEFORE submitting 20_pipeline.sh.
#
#   # Activate the SAME env the jobs use first, so `requests` is importable and srun
#   # propagates it to the compute node (srun defaults to --export=ALL):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
#   ACCOUNT=MYGROUP-SL3-CPU bash scripts/csd3/00_check_network.sh
#
# It probes BOTH node types the harvest uses: `icelake` (discover + triage) AND
# `icelake-himem` (fetch runs inside 20_pipeline.sh on himem). Restrict with e.g.
# PARTITION=icelake.
#
# PASS  -> submit the pipeline as a batch job as normal.
# FAIL  -> see "Do compute nodes have outbound internet?" in scripts/csd3/README.md.
set -uo pipefail

ACCOUNT="${ACCOUNT:-CHANGEME-SL3-CPU}"                 # find yours with: mybalance
PARTITIONS="${PARTITION:-icelake icelake-himem}"       # both node types the harvest uses

if [[ "$ACCOUNT" == CHANGEME-* ]]; then
    echo "Set ACCOUNT first, e.g.:  ACCOUNT=MYGROUP-SL3-CPU bash $0" >&2
    exit 2
fi

# Pre-flight on THIS (login) node: if `requests` is not importable here, the compute-node
# probe would die with a ModuleNotFoundError that reads like a network failure. Catch it
# now with an actionable message instead of burning an srun allocation on a false FAIL.
# srun propagates this same environment to the job (--export=ALL is the default).
if ! python -c 'import requests' 2>/dev/null; then
    echo "ERROR: 'requests' is not importable in the current environment." >&2
    echo "Activate the harvest env first, then re-run, e.g.:" >&2
    echo "  module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate" >&2
    exit 2
fi

rc=0
for PARTITION in $PARTITIONS; do
    echo "=== testing outbound HTTPS to zenodo.org from a '$PARTITION' compute node ==="
    srun -A "$ACCOUNT" -p "$PARTITION" --nodes=1 --ntasks=1 --time=00:05:00 \
        python - <<'PY'
import socket, sys
print("hostname:", socket.gethostname())
try:
    import requests
    r = requests.get("https://zenodo.org/api/records", params={"size": 1}, timeout=30)
    ok = r.status_code == 200 and "hits" in r.json()
    print("HTTP", r.status_code, "| hits reachable:", "hits" in r.json())
    print("PASS: compute node can reach the Zenodo API." if ok
          else "FAIL: unexpected status; check for a proxy requirement.")
    sys.exit(0 if ok else 1)
except Exception as exc:
    print(f"FAIL: {type(exc).__name__}: {exc}")
    print("Compute node may be firewalled — see scripts/csd3/README.md.")
    sys.exit(1)
PY
    prc=$?
    echo "=== $PARTITION: $([[ $prc -eq 0 ]] && echo PASS || echo FAIL) (srun exit $prc) ==="
    [[ $prc -ne 0 ]] && rc=$prc
done

echo "=== overall: $([[ $rc -eq 0 ]] && echo 'PASS on all partitions' || echo 'FAIL — see above') ==="
exit "$rc"
