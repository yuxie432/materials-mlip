#!/bin/bash
# ONE-OFF precheck: can a CSD3 COMPUTE node reach the Zenodo API?
#
# The CSD3 docs do not state whether compute nodes have outbound internet access, and the
# whole fetch stage depends on it. Run this (interactive, ~1 minute, ~1 core-minute of
# your allocation) BEFORE submitting 20_pipeline.sh.
#
#   bash scripts/csd3/00_check_network.sh
#
# PASS  -> submit the pipeline as a batch job as normal.
# FAIL  -> see "Do compute nodes have outbound internet?" in scripts/csd3/README.md.
set -uo pipefail

ACCOUNT="${ACCOUNT:-CHANGEME-SL3-CPU}"     # find yours with: mybalance
PARTITION="${PARTITION:-icelake}"

if [[ "$ACCOUNT" == CHANGEME-* ]]; then
    echo "Set ACCOUNT first, e.g.:  ACCOUNT=MYGROUP-SL3-CPU bash $0" >&2
    exit 2
fi

echo "Testing outbound HTTPS to zenodo.org from a $PARTITION compute node..."
srun -A "$ACCOUNT" -p "$PARTITION" --nodes=1 --ntasks=1 --time=00:05:00 \
    python - <<'PY'
import socket, sys
print("hostname:", socket.gethostname())
try:
    import requests
    r = requests.get("https://zenodo.org/api/records", params={"size": 1}, timeout=30)
    print("HTTP", r.status_code, "| hits reachable:", "hits" in r.json())
    print("PASS: compute nodes can reach the Zenodo API." if r.status_code == 200
          else "FAIL: unexpected status; check for a proxy requirement.")
    sys.exit(0 if r.status_code == 200 else 1)
except Exception as exc:
    print(f"FAIL: {type(exc).__name__}: {exc}")
    print("Compute nodes may be firewalled — see scripts/csd3/README.md.")
    sys.exit(1)
PY
