"""Offline record signals + the tier rule (pure functions; no network).

Every census record gets the same set of cheap, independent signals, computed locally so none of
Zenodo's search quirks apply (HTML is stripped and entities — ``&nbsp;`` included — are decoded
before tokenising; every phrase is written to match its plural):

* **text** — VASP words (strongest), DFT-method words, MLIP words, materials words, and chemical
  formulas (``LiNiO2``, ``CdTe``) in the title/keywords; plus other-domain words and file types
  (genomics, neuroimaging, GIS, audio/video, classical-MD trajectories) as negatives;
* **identity** — the depositor account (``owners``), an ORCID, or a distinctive creator name shared
  with a record already in the VASP dataset; membership of a community that holds one;
* **files** — a loose VASP primary, or file names that say VASP (``vasp``/``vasprun``/``outcar``,
  strong) or DFT-workflow things (``dft``, ``scf``, ``neb``, ``aimd``, ``phonon``, ``relaxation`` … —
  weak);
* **papers** (filled from :mod:`zenodo_census.links`) — a linked or citing paper that cites the VASP
  method papers, or sits in a materials/physics/chemistry field; a Europe PMC data-availability
  mention by a VASP paper.

Measured on the 303 records already in the Zenodo VASP dataset (2026-09-25): 97% reach the top two
tiers on text alone; leave-one-out, 43% share a depositor and 46% an ORCID with another VASP record;
26% link a paper DOI, and 72% of those papers cite the VASP method papers (OpenAlex). On a random
sample of archive-bearing records, ~2.7% reach the top two text tiers.

Tiers: ``T1`` strong (peeked; archives a peek cannot settle are fetched fail-safe), ``T2`` plausible
(peeked; fetched only on positive evidence), ``T3`` low-signal (sampled to measure the residual
blind spot), ``T0`` another domain (skipped; a small control sample validates the filter).
"""

from __future__ import annotations

import html
import re
import unicodedata
from typing import Any, Iterable

from zenodo_harvest.fetch import _PARSE_RE, _PRIMARY_ROLES, _is_junk_member, _unit_role

TIERS = ("T1", "T2", "T3", "T0")

# ---------------------------------------------------------------------------
# text normalisation
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")


def clean_text(s: str | None) -> str:
    """Strip HTML tags, decode entities, turn every Unicode space (``&nbsp;``, NBSP, thin spaces …)
    into a plain one and every dash into ``-``, and drop invisible format characters (soft
    hyphens), so words glued or split by markup read as a reader would read them."""
    if not s:
        return ""
    s = html.unescape(_TAG_RE.sub(" ", s))
    out = []
    for ch in s:
        cat = unicodedata.category(ch)
        if cat in ("Zs", "Cc"):
            out.append(" ")
        elif cat == "Pd" or ch == "\u2212":      # any dash / minus sign -> ASCII hyphen
            out.append("-")
        elif cat != "Cf":                           # drop soft hyphens, zero-width joiners …
            out.append(ch)
    return _SPACE_RE.sub(" ", "".join(out)).strip()


def _subject_text(subjects: Iterable[Any] | None) -> str:
    out = []
    for s in subjects or []:
        if isinstance(s, dict):
            out.append(str(s.get("term") or s.get("subject") or ""))
        else:
            out.append(str(s))
    return " ".join(out)


def record_text(meta: dict[str, Any]) -> tuple[str, str]:
    """``(headline, full)``: title + keywords + subjects (+ journal title), and that plus the
    description and notes — all cleaned."""
    kw = " ".join(str(k) for k in meta.get("keywords") or [])
    journal = (meta.get("journal") or {}).get("title") or ""
    head = clean_text(" ".join([meta.get("title") or "", kw, _subject_text(meta.get("subjects")),
                                journal]))
    full = " ".join([head, clean_text(meta.get("description")), clean_text(meta.get("notes"))])
    return head, full


# ---------------------------------------------------------------------------
# vocabularies (case-insensitive, whole words, plural-safe)
# ---------------------------------------------------------------------------

def _words(*alts: str) -> re.Pattern[str]:
    """Whole-word alternatives; a hyphen counts as a word break ("VASP-based", "DFT-computed")."""
    return re.compile(r"(?<!\w)(?:" + "|".join(alts) + r")(?!\w)", re.IGNORECASE)


_S = r"[\s-]?"   # optional space/hyphen between the parts of a compound term

VASP_RE = _words(r"vasp", r"vasprun(?:\.xml)?", r"outcars?", r"oszicar", r"vaspout(?:\.h5)?",
                 r"vaspkit", r"py4vasp", rf"vienna{_S}ab{_S}initio(?:{_S}simulation{_S}package)?")
DFT_RE = _words(
    r"dft\s?\+\s?u", r"dft", rf"density{_S}functionals?(?:{_S}theory)?", rf"first{_S}principles?",
    rf"ab{_S}initio", r"pbe(?:sol|0)?", r"hse(?:0?6|0?3)?", r"r2scan", r"rscan",
    rf"scan{_S}(?:functional|meta{_S}gga)", rf"meta{_S}gga", r"gga", rf"hybrid{_S}functionals?",
    rf"exchange{_S}correlation", rf"pseudo{_S}potentials?",
    rf"projector{_S}augmented{_S}waves?", r"paw", rf"plane{_S}waves?",
    rf"potential{_S}energy{_S}surfaces?",
    rf"hubbard(?:{_S}u)?", rf"k{_S}points?", rf"monkhorst(?:{_S}pack)?",
    rf"electronic{_S}structures?", rf"band{_S}structures?", rf"densit(?:y|ies){_S}of{_S}states",
    r"phonons?", r"aimd", rf"born{_S}oppenheimer", rf"nudged{_S}elastic{_S}bands?",
    rf"climbing{_S}image", r"neb", rf"formation{_S}energ(?:y|ies)",
    rf"(?:charge{_S})?transition{_S}levels?", rf"adsorption{_S}energ(?:y|ies)",
    rf"cohesive{_S}energ(?:y|ies)", rf"elastic{_S}constants?", rf"work{_S}functions?",
    r"bader", r"lobster", r"cohp", r"phonopy", r"pymatgen", r"atomate2?",
    rf"quantum{_S}espresso", r"castep", r"abinit", r"cp2k", r"siesta", r"gpaw", r"wien2k",
    rf"fhi{_S}aims", rf"dft{_S}d3", r"grimme", rf"spin{_S}orbit{_S}coupling",
    rf"hellmann{_S}feynman", rf"kohn{_S}sham")
MLIP_RE = _words(
    rf"machine{_S}learn(?:ed|ing){_S}(?:interatomic{_S})?potentials?",
    rf"interatomic{_S}potentials?", r"mlips?", rf"neural{_S}network{_S}potentials?",
    r"deepmd(?:-kit)?", rf"deep{_S}potentials?", r"mace", r"nequip", r"allegro",
    rf"gaussian{_S}approximation{_S}potentials?", rf"moment{_S}tensor{_S}potentials?",
    rf"atomic{_S}cluster{_S}expansion", r"chgnet", r"m3gnet", r"sevennet", r"mattersim",
    rf"universal{_S}(?:interatomic{_S})?potentials?", rf"foundation{_S}potentials?")
MATERIALS_RE = _words(
    # specific to condensed matter / materials; generic words (surface, interface, lattice,
    # crystal, adsorption, diffusion, nanoparticle …) are left out — measured on the keyword
    # harvest they pulled in 47-90% non-DFT records (docs/survey-findings.md, 3rd investigation)
    r"supercells?", rf"unit{_S}cells?", rf"crystal{_S}structures?", r"defects?", r"vacanc(?:y|ies)",
    r"interstitials?", r"dopants?", r"doping", r"dislocations?", rf"grain{_S}boundar(?:y|ies)",
    rf"stacking{_S}faults?", r"perovskites?", r"spinels?", r"garnets?", r"olivines?",
    rf"rock{_S}salt", r"wurtzite", rf"zinc{_S}blende", r"kesterites?", r"chalcogenides?",
    r"chalcopyrites?", r"pnictides?", r"semiconductors?", r"insulators?", r"oxides?", r"nitrides?",
    r"carbides?", r"borides?", r"sulfides?", r"sulphides?", r"selenides?", r"tellurides?",
    r"halides?", r"hydrides?", r"alloys?", r"intermetallics?", rf"high{_S}entropy", r"cataly\w*",
    r"electrocatal\w*", r"photocatal\w*", r"adsorbates?", r"slabs?",
    r"monolayers?", rf"2d{_S}materials?", r"graphene", r"mxenes?", r"hbn", r"tmds?",
    rf"transition{_S}metal{_S}dichalcogenides?", rf"van{_S}der{_S}waals", r"heterostructures?",
    r"heterojunctions?", r"nanosheets?", r"nanoribbons?", r"batter(?:y|ies)", r"cathodes?",
    r"anodes?", r"electrolytes?", rf"ionic{_S}conduct\w*", r"thermoelectric\w*",
    r"photovoltaic\w*", rf"solar{_S}cells?", r"ferroelectric\w*", r"piezoelectric\w*",
    r"multiferroic\w*", r"ferromagnet\w*", r"antiferromagnet\w*", r"spintronic\w*",
    r"superconduct\w*", rf"topological{_S}(?:insulators?|semimetals?|materials?|phases?)",
    r"polarons?", r"excitons?", rf"band{_S}gaps?", r"bandgaps?", rf"phase{_S}diagrams?",
    rf"thin{_S}films?", r"polymorphs?", r"zeolites?", rf"metal{_S}organic{_S}frameworks?", r"mofs?",
    rf"point{_S}defects?", rf"space{_S}groups?", r"icsd", r"oqmd", rf"materials{_S}project",
    r"mp-\d+", rf"molten{_S}salts?", rf"liquid{_S}metals?", r"nanoclusters?", r"electrides?")
NEGATIVE_RE = _words(
    r"genes?", r"genom\w*", r"proteins?", r"proteom\w*", r"transcriptom\w*", r"metabolom\w*",
    r"rna", r"dna", r"sequencing", r"patients?", r"clinical", r"cohorts?", r"mice", r"mouse",
    r"animals?", r"ecolog\w*", r"biodiversity", r"microbiom\w*", r"bacteri\w*", r"viral",
    r"virus\w*", r"covid\w*", r"sars", r"neurons?", r"neuronal", rf"neural{_S}activity", r"brain",
    r"fmri", r"eeg", r"cancer", r"tumou?rs?", r"diseases?", r"hospitals?", r"medical",
    r"psycholog\w*", r"students?", r"surveys?", r"interviews?", r"questionnaires?",
    r"sociolog\w*", r"economi\w*", r"elections?", r"parliament\w*", rf"language{_S}models?",
    r"llms?", r"nlp", r"speech", r"music\w*", r"seismic\w*", r"earthquakes?", r"climate",
    r"rainfall", r"ocean\w*", r"hydrolog\w*", r"galax\w*", r"telescopes?", r"astronom\w*",
    r"archaeolog\w*", r"linguistic\w*", r"soils?", r"agricultur\w*", r"crops?", r"fish\w*",
    r"birds?", r"insects?", r"taxonom\w*", r"phylogen\w*", r"wildlife", rf"land{_S}cover",
    rf"remote{_S}sensing", r"traffic", r"pharmac\w*", r"drugs?", r"enzymes?",
    rf"cells?{_S}lines?", r"tissues?", r"twitter", r"tweets?", rf"social{_S}media")
# Record files whose TYPE says "another domain": genomics, neuro/medical imaging, GIS, audio,
# climate grids, phylogenetics, classical-MD (GROMACS/AMBER/NAMD) trajectories. (Not video: MD
# movies are common supplementary material in materials deposits.)
_NEG_FILE_SUFFIXES = (
    ".fastq", ".fq", ".bam", ".sam", ".cram", ".vcf", ".bcf", ".fasta", ".fa", ".fna", ".faa",
    ".gff", ".gff3", ".gtf", ".bed", ".bigwig", ".bw", ".h5ad", ".loom", ".mzml", ".mzxml",
    ".nii", ".dcm", ".edf", ".bdf", ".fif", ".shp", ".shx", ".gpkg", ".geojson", ".kml", ".kmz",
    ".nc", ".grib", ".grb", ".wav", ".mp3", ".flac", ".ogg",
    ".xtc", ".trr", ".tpr", ".gro", ".dcd", ".psf", ".prmtop", ".inpcrd", ".nwk", ".newick")
_COMPRESS_SUFFIXES = (".gz", ".bz2", ".xz", ".zst", ".zip")

# ---------------------------------------------------------------------------
# chemical formulas
# ---------------------------------------------------------------------------

ELEMENTS = frozenset((
    "H He Li Be B C N O F Ne Na Mg Al Si P S Cl Ar K Ca Sc Ti V Cr Mn Fe Co Ni Cu Zn Ga Ge As Se "
    "Br Kr Rb Sr Y Zr Nb Mo Tc Ru Rh Pd Ag Cd In Sn Sb Te I Xe Cs Ba La Ce Pr Nd Pm Sm Eu Gd Tb Dy "
    "Ho Er Tm Yb Lu Hf Ta W Re Os Ir Pt Au Hg Tl Pb Bi Po At Rn Fr Ra Ac Th Pa U Np Pu Am Cm Bk Cf "
    "Es Fm Md No Lr").split())
# Candidate tokens are cheap to find (no nested quantifiers); each is then segmented into
# element symbols by a linear scan. (A single regex for "element + optional count" backtracks
# exponentially on runs like "OxOxOx…", where x can be a lower-case letter or a stoichiometry.)
_FORMULA_CANDIDATE = re.compile(r"(?<![\w-])[A-Z][A-Za-z0-9.δ]{1,40}(?![\w])")
# element-valid tokens that are common words/acronyms elsewhere (biology, generic chemistry)
_FORMULA_STOP = frozenset({"NaCl", "NaN", "HeLa", "CoV", "CoA", "NaOH", "KCl", "HCl", "CaCl2",
                           "MgCl2", "NaHCO3", "CaCO3", "CoLi", "BiOS", "BaSe", "CaSe", "CuBi",
                           "NiCe", "HoW", "SnOw", "PoP", "NoV", "YeS", "CoNf"})


def _formula_symbols(tok: str) -> list[str] | None:
    """The element symbols of ``tok`` if it reads as a formula — element symbols, each optionally
    followed by a count (``2``, ``0.5``) or a stoichiometry variable (x, y, z, δ) — else None.
    Deterministic left-to-right scan (a two-letter symbol wins over a one-letter one)."""
    syms: list[str] = []
    i, n = 0, len(tok)
    while i < n:
        if not tok[i].isupper():
            return None
        if i + 1 < n and tok[i + 1].islower() and tok[i:i + 2] in ELEMENTS:
            sym, i = tok[i:i + 2], i + 2
        elif tok[i] in ELEMENTS:
            sym, i = tok[i], i + 1
        else:
            return None
        syms.append(sym)
        if i < n and tok[i].isdigit():
            while i < n and (tok[i].isdigit() or (tok[i] == "." and i + 1 < n
                                                   and tok[i + 1].isdigit())):
                i += 1
        elif i < n and tok[i] in "xyzδ":
            i += 1
    return syms


def formula_hits(text: str) -> list[str]:
    """Chemical-formula-looking tokens: >= 2 element symbols, all real elements, at least one
    lower-case letter (so all-caps acronyms like ``SSP5``/``CSV`` don't count; ``CO2``/``H2O``
    fall out the same way, which is intended — they are not materials cues). ``TiOx``, ``MoS2``,
    ``InP`` and ``SrTiO3`` (of ``SrTiO3-δ``) count."""
    out = []
    for tok in _FORMULA_CANDIDATE.findall(text or ""):
        tok = tok.rstrip(".")
        if tok in _FORMULA_STOP or not re.search(r"[a-z]", tok):
            continue
        syms = _formula_symbols(tok)
        if syms is not None and len(syms) >= 2:
            out.append(tok)
    return out


# ---------------------------------------------------------------------------
# files
# ---------------------------------------------------------------------------

FILE_VASP_RE = re.compile(r"(?:^|[^a-z])(vasp|vasprun|outcar)(?:[^a-z]|$)", re.IGNORECASE)
# calculation-workflow words that are rare outside DFT (generic ones — "calculations", "relax",
# "bulk", "bands", "dos" — matched a biology tool called RELAX and "Ellipsoid-Calculations.zip")
FILE_HINT_RE = re.compile(
    r"(?:^|[^a-z])(dft|scf|nscf|neb|aimd|phonons?|supercells?|slabs?|relaxations?|defect_calcs?|"
    r"poscars?|contcars?|incars?|kpoints)(?:[^a-z]|$)", re.IGNORECASE)


def _key_base(f: dict[str, Any]) -> str:
    return str(f.get("key") or "").rsplit("/", 1)[-1]


def is_loose_primary(f: dict[str, Any]) -> bool:
    """A directly-exposed file the shared fetch would download AND seed a calc unit from."""
    key = str(f.get("key") or "")
    base = key.rsplit("/", 1)[-1]
    return (not _is_junk_member(key) and bool(_PARSE_RE.search(base))
            and _unit_role(base) in _PRIMARY_ROLES)


def negative_files(files: list[dict[str, Any]]) -> list[str]:
    out = []
    for f in files:
        low = _key_base(f).lower()
        for c in _COMPRESS_SUFFIXES:
            if low.endswith(c):
                low = low[: -len(c)]
                break
        if low.endswith(_NEG_FILE_SUFFIXES):
            out.append(_key_base(f))
    return out


# ---------------------------------------------------------------------------
# paper links
# ---------------------------------------------------------------------------

DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s\"'<>\[\]{},;]+)", re.IGNORECASE)
# The VASP method papers themselves (a record that cites/references one says "this is VASP data").
VASP_DOIS = frozenset({"10.1103/physrevb.54.11169", "10.1016/0927-0256(96)00008-0",
                       "10.1103/physrevb.59.1758", "10.1103/physrevb.50.17953",
                       "10.1103/physrevb.47.558", "10.1103/physrevb.49.14251"})
ARXIV_RE = re.compile(r"(?:arxiv\.org/(?:abs|pdf)/|arxiv:\s*)(\d{4}\.\d{4,5})", re.IGNORECASE)
# DOI prefixes of data repositories (a dataset, not a paper — no reference list to test).
REPO_DOI_PREFIXES = ("10.5281/", "10.24435/", "10.17172/", "10.6084/", "10.17632/", "10.5061/",
                     "10.18419/", "10.4121/", "10.7910/", "10.57760/", "10.11583/", "10.18126/",
                     "10.25740/", "10.15151/", "10.5517/")
# relations that link a record to another version/part of itself, not to a paper
_SELF_RELATIONS = {"isversionof", "hasversion", "isnewversionof", "ispreviousversionof",
                   "ispartof", "haspart", "isidenticalto", "isalternateidentifier",
                   "isvariantformof", "isoriginalformof", "obsoletes", "isobsoletedby"}


def norm_doi(d: str) -> str | None:
    """A bare lower-case DOI from a DOI string / DOI URL / ``doi:`` form, or None. Parentheses are
    kept when balanced (``10.1016/0927-0256(96)00008-0``) and an unbalanced trailing ``)`` — a
    DOI written inside brackets — is dropped, as are query strings, fragments and punctuation."""
    d = html.unescape(d).strip()
    d = re.sub(r"^(?:https?://)?(?:www\.)?(?:dx\.)?doi\.org/", "", d, flags=re.IGNORECASE)
    d = re.sub(r"^doi:\s*", "", d, flags=re.IGNORECASE)
    d = re.split(r"[?#]", d, maxsplit=1)[0]
    d = d.rstrip(".,;:")
    while d.endswith(")") and d.count(")") > d.count("("):
        d = d[:-1].rstrip(".,;:")
    d = re.sub(r"(?:/full|/abstract|/pdf|\.full|\.pdf|/epdf|/meta)$", "", d, flags=re.IGNORECASE)
    d = d.lower()
    if not re.match(r"^10\.\d{4,9}/\S+$", d):
        return None
    return d


def paper_dois(meta: dict[str, Any]) -> list[str]:
    """Paper DOIs a record points at, most direct first: related identifiers (DOI / URL / arXiv
    schemes, minus self-relations), then DOIs and arXiv ids in the description (hrefs included), the
    notes and the reference list — deduplicated in that order, so a cap keeps the depositor's own
    links. Data-repository DOIs are dropped; arXiv ids become ``10.48550/arxiv.*`` DOIs."""
    found: dict[str, None] = {}

    def add(d: str | None) -> None:
        if d and not d.startswith(REPO_DOI_PREFIXES):
            found.setdefault(d, None)

    for ri in meta.get("related_identifiers") or []:
        rel = str(ri.get("relation") or "").lower()
        if rel in _SELF_RELATIONS:
            continue
        ident = str(ri.get("identifier") or "")
        scheme = str(ri.get("scheme") or "").lower()
        if scheme == "doi":
            add(norm_doi(ident))
        elif scheme == "arxiv":
            m = re.search(r"(\d{4}\.\d{4,5})", ident)
            if m:
                add(f"10.48550/arxiv.{m.group(1)}")
        else:
            for d in DOI_RE.findall(ident):
                add(norm_doi(d))
            for a in ARXIV_RE.findall(ident):
                add(f"10.48550/arxiv.{a}")
    blobs = [meta.get("description") or "", meta.get("notes") or "",
             " ".join(str(r) for r in meta.get("references") or [])]
    for blob in blobs:
        blob = html.unescape(blob)
        for d in DOI_RE.findall(blob):
            add(norm_doi(d))
        for a in ARXIV_RE.findall(blob):
            add(f"10.48550/arxiv.{a}")
    return list(found)


# ---------------------------------------------------------------------------
# identities
# ---------------------------------------------------------------------------

def norm_name(name: str) -> str | None:
    """``"last|f"`` from ``"Last, First"`` or ``"First Last"`` (ASCII-folded, lower-case), or None
    for a single-token name. (Organisation names are not detected; they rarely collide with the
    census-wide frequency cap applied to names.)"""
    n = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    n = re.sub(r"[^a-z, ]", " ", n)
    if "," in n:
        last, first = n.split(",", 1)
    else:
        parts = n.split()
        if len(parts) < 2:
            return None
        last, first = parts[-1], " ".join(parts[:-1])
    last, first = _SPACE_RE.sub(" ", last).strip(), first.strip()
    if not last or not first or len(last) < 2:
        return None
    return f"{last}|{first[0]}"


def owner_ids(rec: dict[str, Any]) -> list[str]:
    out = []
    for o in rec.get("owners") or []:
        oid = o.get("id") if isinstance(o, dict) else o
        if oid is not None:
            out.append(str(oid))
    return out


def people(meta: dict[str, Any]) -> list[dict[str, Any]]:
    return list(meta.get("creators") or []) + list(meta.get("contributors") or [])


def community_ids(meta: dict[str, Any]) -> list[str]:
    return [str(c.get("id") if isinstance(c, dict) else c) for c in meta.get("communities") or []
            if (c.get("id") if isinstance(c, dict) else c)]


# ---------------------------------------------------------------------------
# licence
# ---------------------------------------------------------------------------

_NO_LICENSE_IDS = {"", "notspecified", "all-rights-reserved", "arr", "closed", "restricted",
                   "copyright", "none", "other-closed", "proprietary"}


def licence_id(meta: dict[str, Any]) -> str | None:
    lic = meta.get("license")
    if isinstance(lic, dict):
        lic = lic.get("id")
    return str(lic) if lic else None


def licence_class(lic: str | None) -> str:
    """``open`` (CC0/BY/BY-SA/permissive), ``nc`` (NonCommercial, derivatives allowed), ``nd``
    (NoDerivatives, any), or ``none`` (no licence / all rights reserved). The current dataset
    admits ``open`` + ``nc`` (the NC expansion); ``nd``/``none`` records are listed for review."""
    if lic is None:
        return "none"
    norm = lic.strip().lower()
    toks = set(re.split(r"[-_.\s]+", norm))
    if norm in _NO_LICENSE_IDS or toks & {"closed", "proprietary"}:
        return "none"
    if "nd" in toks:
        return "nd"
    if "nc" in toks:
        return "nc"
    return "open"


ADMITTED_LICENCES = ("open", "nc")


# ---------------------------------------------------------------------------
# signals + tier
# ---------------------------------------------------------------------------

# OpenAlex fields whose papers make a sparse Zenodo record plausibly computational-materials.
MATSCI_FIELDS = frozenset({"Materials Science", "Physics and Astronomy", "Chemistry",
                           "Chemical Engineering", "Energy", "Engineering"})


class Seeds:
    """Identities of the records already in the VASP dataset (and optional extra seed names).

    ``name_df`` / ``community_size`` are census-wide counts used to ignore common names and huge
    generic communities (a shared ``"wang|y"`` or membership of the ``eu`` community says
    nothing); ``owner_size`` flags institutional accounts that deposit hundreds of records."""

    def __init__(self) -> None:
        self.owners: set[str] = set()
        self.orcids: set[str] = set()
        self.names: set[str] = set()
        self.communities: set[str] = set()
        self.name_df: dict[str, int] = {}
        self.community_size: dict[str, int] = {}
        self.owner_size: dict[str, int] = {}
        self.orcid_size: dict[str, int] = {}

    def add_record(self, rec: dict[str, Any]) -> None:
        meta = rec.get("metadata") or {}
        self.owners.update(owner_ids(rec))
        for p in people(meta):
            if p.get("orcid"):
                self.orcids.add(str(p["orcid"]))
            nn = norm_name(str(p.get("name") or ""))
            if nn:
                self.names.add(nn)
        self.communities.update(community_ids(meta))

    def count(self, rec: dict[str, Any]) -> None:
        """Census-wide frequency pass (call for every census record)."""
        meta = rec.get("metadata") or {}
        for nn in {n for p in people(meta) if (n := norm_name(str(p.get("name") or "")))}:
            self.name_df[nn] = self.name_df.get(nn, 0) + 1
        for c in set(community_ids(meta)):
            self.community_size[c] = self.community_size.get(c, 0) + 1
        for o in set(owner_ids(rec)):
            self.owner_size[o] = self.owner_size.get(o, 0) + 1
        for oc in {str(p["orcid"]) for p in people(meta) if p.get("orcid")}:
            self.orcid_size[oc] = self.orcid_size.get(oc, 0) + 1


NAME_DF_MAX = 30          # a creator name on more census records than this is too common to use
COMMUNITY_MAX = 3000      # a community bigger than this is generic (eu, zenodo, …)
OWNER_BULK = 300          # an account owning more census records than this is institutional


def _stem_word(w: str) -> str:
    if len(w) > 4 and w.endswith("ies"):
        return w[:-3] + "y"
    if len(w) > 4 and w.endswith(("ses", "xes")):
        return w[:-2]
    if len(w) > 3 and w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


def term_stem(term: str) -> str:
    """One key per vocabulary term regardless of inflection or joining: "phonons" = "phonon",
    "k-points" = "k point", "densities of states" = "density of state" — so distinct-term counts
    (which gate the tiers) are not inflated by a plural and its singular both appearing."""
    return " ".join(_stem_word(w) for w in re.split(r"[\s-]+", term.lower()) if w)


def text_signals(rec: dict[str, Any]) -> dict[str, Any]:
    """The pure text/file part of the signals (no seeds, no papers). Vocabulary hits are reported
    as distinct stems (:func:`term_stem`)."""
    meta = rec.get("metadata") or {}
    head, full = record_text(meta)
    files = rec.get("files") or []
    desc_len = len(clean_text(meta.get("description")))

    def uniq(rx: re.Pattern[str], text: str) -> list[str]:
        return sorted({term_stem(m.group(0)) for m in rx.finditer(text)})

    names = [_key_base(f) for f in files]
    return {
        "vasp": uniq(VASP_RE, full),
        "dft": uniq(DFT_RE, full),
        "mlip": uniq(MLIP_RE, full),
        "materials": uniq(MATERIALS_RE, full),
        "formula": sorted(set(formula_hits(head))),
        "negative": uniq(NEGATIVE_RE, full),
        "neg_files": negative_files(files)[:5],
        "file_vasp": sorted({n for n in names if FILE_VASP_RE.search(n)})[:5],
        "file_hint": sorted({n for n in names if FILE_HINT_RE.search(n)})[:5],
        "loose_primary": sorted({_key_base(f) for f in files if is_loose_primary(f)})[:5],
        "desc_len": desc_len,
    }


def identity_signals(rec: dict[str, Any], seeds: Seeds) -> dict[str, Any]:
    meta = rec.get("metadata") or {}
    owners = [o for o in owner_ids(rec) if o in seeds.owners]
    all_orcids = {str(p["orcid"]) for p in people(meta)
                  if p.get("orcid") and str(p["orcid"]) in seeds.orcids}
    # a seed author on hundreds of census records (institutional-scale output) says less
    orcids = sorted(o for o in all_orcids if seeds.orcid_size.get(o, 0) <= OWNER_BULK)
    names = sorted({nn for p in people(meta)
                    if (nn := norm_name(str(p.get("name") or ""))) and nn in seeds.names
                    and seeds.name_df.get(nn, 0) <= NAME_DF_MAX})
    comms = sorted({c for c in community_ids(meta) if c in seeds.communities
                    and seeds.community_size.get(c, 0) <= COMMUNITY_MAX})
    bulk_owner = any(seeds.owner_size.get(o, 0) > OWNER_BULK for o in owners)
    return {"owner": owners, "orcid": orcids, "name": names, "community": comms,
            "bulk_owner": bulk_owner, "bulk_orcid": bool(all_orcids) and not orcids}


# DFT-list words that also mean something common elsewhere (discrete Fourier transform, health &
# safety, New England Biolabs, animal paws, the crustacean, surnames, a nap …) — as stems: alone
# they are a weak cue that cannot overrule another-domain evidence; with a second DFT term or a
# materials cue they are as good as any. Same idea for two MLIP names (cardiac MACE, music).
AMBIGUOUS_DFT = frozenset({"dft", "hse", "gga", "paw", "neb", "bader", "lobster", "grimme",
                           "siesta", "phonon"})
AMBIGUOUS_MLIP = frozenset({"mace", "allegro"})


def tier_of(sig: dict[str, Any]) -> tuple[str, list[str]]:
    """``(tier, reasons)`` from a record's merged signals (text + identity + paper keys:
    ``paper_vasp`` / ``vasp_paper_linked`` (lists), ``paper_fields`` (list), ``epmc`` (list)).

    T1 = a strong cue on a record that is not clearly another domain; T2 = weaker cues (or a strong
    but ambiguous one — "VASP" is also a cell-biology protein — on a record whose other words say
    another domain: peeked, fetched only on evidence); T0 = another domain with nothing to
    contradict it; T3 = no cue either way."""
    dft = [term_stem(t) for t in sig.get("dft") or []]
    n_dft, n_mat = len(set(dft)), len(sig.get("materials") or [])
    dft_reliable = n_dft >= 2 or any(t not in AMBIGUOUS_DFT for t in dft)
    mlip = [term_stem(t) for t in sig.get("mlip") or []]
    mlip_reliable = any(t not in AMBIGUOUS_MLIP for t in mlip)
    formula = bool(sig.get("formula"))
    n_neg = len(sig.get("negative") or []) + min(len(sig.get("neg_files") or []), 3)
    content_mat = n_mat >= 1 or formula
    paper_mat = bool(set(sig.get("paper_fields") or []) & MATSCI_FIELDS)
    # "another domain" = negative words / file types that outnumber the materials cues, with no
    # reliable DFT wording or formula to contradict them
    negative = n_neg >= 1 and n_neg > n_mat and not dft_reliable and not formula

    strong: list[str] = []
    if sig.get("loose_primary"):
        strong.append("loose_vasp_file")
    if sig.get("vasp_paper_linked"):
        strong.append("links_vasp_paper")
    if sig.get("paper_vasp"):
        strong.append("paper_cites_vasp")
    if sig.get("epmc"):
        strong.append("epmc_mention")
    if dft_reliable and (content_mat or n_dft >= 2):
        strong.append("text_dft_materials")
    elif n_dft and (n_mat >= 2 or formula) and not negative:
        strong.append("text_dft_materials")          # an ambiguous DFT word + clear materials
    if mlip and (content_mat or dft_reliable) and (mlip_reliable or dft_reliable):
        strong.append("text_mlip_materials")
    # name-level cues that are ambiguous on their own (the VASP protein; "vasp" in a file name)
    ambiguous_strong = []
    if sig.get("vasp"):
        ambiguous_strong.append("text_vasp")
    if sig.get("file_vasp"):
        ambiguous_strong.append("file_vasp")
    identity_strong = []
    if sig.get("owner") and not sig.get("bulk_owner"):
        identity_strong.append("seed_owner")
    if sig.get("orcid"):
        identity_strong.append("seed_orcid")
    if strong:
        return "T1", strong + ambiguous_strong + identity_strong
    if (ambiguous_strong or identity_strong) and not negative:
        return "T1", ambiguous_strong + identity_strong

    weak: list[str] = []
    if n_dft:
        weak.append("text_dft")
    if mlip:
        weak.append("text_mlip")
    if n_mat >= 2:
        weak.append("text_materials")
    if formula:
        weak.append("formula")
    if n_mat == 1 and int(sig.get("desc_len") or 0) < 300:
        weak.append("sparse_with_materials_cue")
    if paper_mat:
        weak.append("paper_field")
    if sig.get("community"):
        weak.append("seed_community")
    if sig.get("file_hint"):
        weak.append("file_hint")
    if sig.get("owner") and sig.get("bulk_owner"):
        weak.append("seed_bulk_owner")
    if sig.get("bulk_orcid"):
        weak.append("seed_bulk_orcid")
    # a shared creator NAME is noisy (surname + initial): it counts only beside a content cue
    if sig.get("name") and (weak or n_mat):
        weak.append("seed_name")
    # a strong-but-ambiguous cue on a negative-looking record is still worth a peek (T2)
    weak.extend(ambiguous_strong)
    weak.extend(identity_strong)
    contradicts_negative = dft_reliable or formula or paper_mat or bool(ambiguous_strong)
    if weak and (not negative or contradicts_negative):
        return "T2", weak
    if negative:
        return "T0", ["negative_" + ("files" if sig.get("neg_files") else "words")]
    return "T3", []
