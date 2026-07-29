#!/usr/bin/env python
"""Measure pymatgen's parse-time peak RSS on CSD3, to size --max-primary-bytes.

`parse` loads a whole vasprun.xml/OUTCAR ionic trajectory into RAM. On a batch node
memory is a hard cgroup limit (per core: icelake 3370 MiB, icelake-himem 6760 MiB), and
an over-budget parse is a SIGKILL of the whole job — not a catchable MemoryError. This
tool parses each primary in a FRESH subprocess and reports its peak RSS (ru_maxrss, what
the cgroup measures), so you can set --max-primary-bytes = allocated_RAM / observed_ratio.

Usage (interactive, on a compute node — pick the partition/cores you'll actually parse on):
    srun -A MYACCT-SL3-CPU -p icelake-himem --cpus-per-task=4 --time=00:20:00 \
        python scripts/csd3/csd3_parse_memory.py --raw-dir $ZENODO_HARVEST_DATA/raw --top 8
    # before any fetch, calibrate on synthetic samples instead:
    srun ... python scripts/csd3/csd3_parse_memory.py --synthetic

Run from the repo root (so `import zenodo_harvest` works) or pass --repo. Needs the
`parse` extra installed (pymatgen + ase). This is a calibration helper, not a unit test.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _is_primary(name: str) -> bool:
    low = name.lower()
    return (("vasprun" in low and low.endswith((".xml", ".xml.gz", ".xml.bz2", ".xml.xz")))
            or ("vaspout" in low and low.endswith(".h5"))
            or "outcar" in low)


# ---- the child: parse ONE file the way parse.py does, print "<peak_mb> <n_frames>" ----
_CHILD = r"""
import os, resource, sys
sys.path.insert(0, os.environ.get("ZH_REPO", "."))
path = sys.argv[1]
low = path.lower()
try:
    if "outcar" in low and "vasprun" not in low and "vaspout" not in low:
        from zenodo_harvest.parse import _parse_outcar_ase
        frames, _ = _parse_outcar_ase(path, "probe")
    elif low.endswith(".h5"):
        from zenodo_harvest.parse import parse_vaspout
        frames, _ = parse_vaspout(path, "probe", None)
    else:
        from zenodo_harvest.parse import parse_vasprun
        frames, _ = parse_vasprun(path, "probe", None)
    n = len(frames)
except Exception as exc:                       # a parse failure is not a memory result
    print(f"ERR {type(exc).__name__}: {exc}")
    sys.exit(3)
peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
print(f"{peak_mb:.1f} {n}")
"""


def _measure(path: Path, repo: str, timeout: int) -> tuple[float, int] | None:
    env = {**os.environ, "ZH_REPO": repo}
    try:
        out = subprocess.run([sys.executable, "-c", _CHILD, str(path)], env=env,
                             capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"  {path.name:<40} TIMEOUT after {timeout}s (too big to parse in time)")
        return None
    if out.returncode != 0:
        print(f"  {path.name:<40} {out.stdout.strip() or out.stderr.strip()[:80]}")
        return None
    peak_mb, n = out.stdout.split()
    return float(peak_mb), int(n)


def _make_synthetic(path: Path, n_atoms: int, n_steps: int) -> None:
    """A trajectory-dominated vasprun.xml (matches what parse builds objects for)."""
    import random
    random.seed(0)
    pos = [(random.random(), random.random(), random.random()) for _ in range(n_atoms)]
    frc = [(random.uniform(-1, 1), random.uniform(-1, 1), random.uniform(-1, 1)) for _ in range(n_atoms)]
    v3 = lambda a: "".join(f"<v>{x:.8f} {y:.8f} {z:.8f} </v>\n" for x, y, z in a)

    def struct(nm=""):
        nm = f' name="{nm}"' if nm else ""
        return (f'<structure{nm}><crystal><varray name="basis">'
                f'<v>10 0 0 </v><v>0 10 0 </v><v>0 0 10 </v></varray>'
                f'<i name="volume">1000</i><varray name="rec_basis">'
                f'<v>.1 0 0 </v><v>0 .1 0 </v><v>0 0 .1 </v></varray></crystal>'
                f'<varray name="positions">\n{v3(pos)}</varray></structure>\n')

    with open(path, "w") as f:
        f.write('<?xml version="1.0"?>\n<modeling>\n'
                '<generator><i type="string" name="version">6.4.2</i></generator>\n')
        f.write('<incar><i type="int" name="NELM">60</i></incar>\n')
        f.write('<kpoints><varray name="kpointlist"><v>0 0 0 </v></varray>'
                '<varray name="weights"><v>1 </v></varray></kpoints>\n')
        f.write('<parameters><separator name="electronic">'
                '<i type="int" name="NELM">60</i></separator></parameters>\n')
        f.write(f'<atominfo><atoms>{n_atoms}</atoms><types>1</types>'
                '<array name="atoms"><dimension dim="1">ion</dimension>'
                '<field type="string">element</field><field type="int">atomtype</field><set>')
        f.write(''.join('<rc><c>Si</c><c>1</c></rc>' for _ in range(n_atoms)))
        f.write('</set></array><array name="atomtypes"><dimension dim="1">type</dimension>'
                '<field type="int">atomspertype</field><field type="string">element</field>'
                '<field>mass</field><field>valence</field><field type="string">pseudopotential</field>'
                f'<set><rc><c>{n_atoms}</c><c>Si</c><c>28.0</c><c>4.0</c><c>PAW_PBE Si</c></rc></set>'
                '</array></atominfo>\n')
        f.write(struct("initialpos"))
        eb = ('<energy><i name="e_fr_energy">-100.1</i><i name="e_wo_entrp">-100.1</i>'
              '<i name="e_0_energy">-100.1</i></energy>\n')
        for _ in range(n_steps):
            f.write('<calculation>')
            for j in range(8):
                f.write('<scstep>' + eb.replace("-100.1", f"{-100.0 - 10.0**-j:.6f}") + '</scstep>')
            f.write(struct())
            f.write('<varray name="forces">\n' + v3(frc) + '</varray>')
            f.write('<varray name="stress"><v>1 0 0 </v><v>0 1 0 </v><v>0 0 1 </v></varray>')
            f.write(eb + '</calculation>\n')
        f.write(struct("finalpos") + '</modeling>\n')


def _synthetic(tmp: Path, repo: str, timeout: int) -> list[float]:
    ratios = []
    for n_atoms, n_steps in [(50, 4000), (200, 8000), (300, 20000)]:
        p = tmp / f"syn_{n_atoms}_{n_steps}.xml"
        _make_synthetic(p, n_atoms, n_steps)
        mb = p.stat().st_size / 1e6
        res = _measure(p, repo, timeout)
        p.unlink()
        if res:
            peak, n = res
            ratios.append(peak / mb)
            print(f"  synthetic {mb:7.1f}MB -> peak {peak:8.1f}MB  ratio {peak/mb:5.1f}x  frames {n}")
    return ratios


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=os.environ.get("ZENODO_HARVEST_DATA", ".") + "/raw",
                    help="fetched staging dir to scan for real vasprun.xml/OUTCAR")
    ap.add_argument("--repo", default=".", help="repo root (for import zenodo_harvest)")
    ap.add_argument("--top", type=int, default=8, help="measure the N biggest primaries")
    ap.add_argument("--timeout", type=int, default=900, help="per-file parse timeout (s)")
    ap.add_argument("--synthetic", action="store_true",
                    help="also measure synthetic sized samples (use before any fetch)")
    args = ap.parse_args()

    # Report the SLURM allocation so the recommendation is anchored to real RAM.
    cpus = int(os.environ.get("SLURM_CPUS_PER_TASK", "0") or 0)
    memc = os.environ.get("SLURM_MEM_PER_CPU")   # MiB, if set
    print(f"host={os.uname().nodename}  SLURM_CPUS_PER_TASK={cpus or '?'}  "
          f"SLURM_MEM_PER_CPU={memc or '?'}MiB")
    alloc_gb = (cpus * int(memc) / 1024.0) if (cpus and memc) else None
    if alloc_gb:
        print(f"allocated RAM this job ~= {alloc_gb:.1f} GB")

    raw = Path(args.raw_dir)
    primaries = (sorted((p for p in raw.rglob("*") if p.is_file() and _is_primary(p.name)),
                        key=lambda p: p.stat().st_size, reverse=True)[:args.top]
                 if raw.is_dir() else [])

    ratios: list[float] = []
    if primaries:
        print(f"\nmeasuring {len(primaries)} largest real primaries under {raw}:")
        for p in primaries:
            mb = p.stat().st_size / 1e6
            res = _measure(p, args.repo, args.timeout)
            if res:
                peak, n = res
                ratios.append(peak / mb)
                print(f"  {p.name:<32} {mb:8.1f}MB -> peak {peak:8.1f}MB  "
                      f"ratio {peak/mb:5.1f}x  frames {n}")
    else:
        print(f"\nno real primaries under {raw}")

    if args.synthetic or not primaries:
        print("\nsynthetic samples:")
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            ratios += _synthetic(Path(td), args.repo, args.timeout)

    if ratios:
        worst = max(ratios)
        print(f"\nobserved peak/size ratio: worst {worst:.1f}x")
        if alloc_gb:
            safe = 0.85 * alloc_gb * 1e9 / worst
            print(f"recommended --max-primary-bytes for THIS allocation "
                  f"(~{alloc_gb:.1f} GB, 15% headroom): {int(safe)}  (~{safe/1e9:.2f} GB)")
        else:
            print("(submit under SLURM, or set SLURM_CPUS_PER_TASK/SLURM_MEM_PER_CPU, "
                  "for a cap recommendation)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
