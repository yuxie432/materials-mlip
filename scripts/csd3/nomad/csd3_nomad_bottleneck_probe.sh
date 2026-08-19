#!/bin/bash
# Launch the NOMAD compute-node bottleneck probe, AND answer the one question the Python probe
# cannot answer from a single node: do TWO compute nodes get DIFFERENT egress public IPs?
#
# That is the make-or-break test for parallelising the NOMAD fetch WITHOUT a rate-limit
# exemption. The `/uploads/{id}/raw` throttle ("1 in-flight connection, new every ~5 s") is
# PER IP. The docs ASSUME CSD3 puts all compute behind one shared NAT (=> extra nodes can't
# help). If that assumption is WRONG and nodes egress via different IPs, then running the
# pipeline on K nodes (each fetching a disjoint slice of uploads into its own dataset dir, then
# merge-datasets) multiplies throughput ~Kx for free. This script checks it directly.
#
#   ACCOUNT=<MYGROUP>-SL3-CPU bash scripts/csd3/nomad/csd3_nomad_bottleneck_probe.sh
#
# Activate the harvest env FIRST (srun propagates it with --export=ALL, the default):
#   module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate
set -uo pipefail

ACCOUNT="${ACCOUNT:-${SBATCH_ACCOUNT:-CHANGEME-SL3-CPU}}"
PARTITION="${PARTITION:-icelake}"          # the probe is network-only; any partition with egress
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"

if [[ "$ACCOUNT" == CHANGEME-* ]]; then
    echo "Set ACCOUNT first, e.g.:  ACCOUNT=MYGROUP-SL3-CPU bash $0" >&2
    exit 2
fi
if ! python -c 'import requests' 2>/dev/null; then
    echo "ERROR: 'requests' not importable. Activate the env first:" >&2
    echo "  module load python/3.11.0-icl && source ~/materials-mlip/.venv/bin/activate" >&2
    exit 2
fi

echo "############################################################################"
echo "# 1) egress public IP from TWO compute nodes (differ => per-node throttle)  #"
echo "############################################################################"
# One task per node on 2 nodes; each prints its hostname + the public IP the NOMAD server sees.
srun -A "$ACCOUNT" -p "$PARTITION" --nodes=2 --ntasks-per-node=1 --time=00:04:00 \
    python - <<'PY'
import socket
import requests
try:
    ip = requests.get("https://api.ipify.org", timeout=20).text.strip()
except Exception as exc:  # noqa: BLE001
    ip = f"<unknown: {type(exc).__name__}>"
print(f"  node {socket.gethostname():<20} egress public IP = {ip}", flush=True)
PY
echo
echo "  -> If the two IPs DIFFER: the fetch can run on K nodes for ~Kx throughput, no exemption."
echo "     If they are the SAME: all compute shares one throttle bucket (multi-node won't help;"
echo "     a login/data-transfer node with its own IP might still add one more parallel stream)."
echo

echo "############################################################################"
echo "# 2) single-node bottleneck decomposition (throughput / pattern / throttle) #"
echo "############################################################################"
srun -A "$ACCOUNT" -p "$PARTITION" --nodes=1 --ntasks=1 --cpus-per-task=1 --time=00:25:00 \
    python -u "$REPO_ROOT/scripts/csd3/nomad/csd3_nomad_bottleneck_probe.py"
