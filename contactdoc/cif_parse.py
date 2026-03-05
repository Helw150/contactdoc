"""mmCIF parsing with Gemmi: chain policy, residue extraction, pLDDT computation."""

from dataclasses import dataclass

import gemmi

CANONICAL_20 = frozenset([
    "ALA", "ARG", "ASN", "ASP", "CYS", "GLN", "GLU", "GLY", "HIS", "ILE",
    "LEU", "LYS", "MET", "PHE", "PRO", "SER", "THR", "TRP", "TYR", "VAL",
])


@dataclass
class Residue:
    index: int  # 1-based
    name: str   # 3-letter code
    plddt: float
    gemmi_residue: object  # gemmi.Residue reference for identity matching


@dataclass
class ParseResult:
    residues: list[Residue]
    chain: object  # gemmi.Chain
    model: object  # gemmi.Model
    structure: object  # gemmi.Structure


def parse_cif(cif_content: str) -> gemmi.Structure:
    doc = gemmi.cif.read_string(cif_content)
    return gemmi.make_structure_from_block(doc[0])


def parse_cif_from_path(path: str) -> gemmi.Structure:
    return gemmi.read_structure(path)


def extract_residues(
    structure: gemmi.Structure,
    require_single_chain: bool = True,
    canonical_residue_policy: str = "map_to_unk",
) -> ParseResult | str:
    """Extract residue list from structure.

    Returns ParseResult on success, or an error reason string on failure.
    """
    if len(structure) == 0:
        return "no_models"

    model = structure[0]

    polymer_chains = [ch for ch in model if ch.get_polymer()]
    if require_single_chain and len(polymer_chains) != 1:
        return f"chain_count_{len(polymer_chains)}"

    chain = polymer_chains[0] if polymer_chains else None
    if chain is None:
        return "no_polymer_chain"

    polymer = chain.get_polymer()
    residues = []

    for idx, res in enumerate(polymer, start=1):
        name = res.name.strip()

        if name not in CANONICAL_20:
            if canonical_residue_policy == "skip_entry":
                return f"noncanonical_residue_{name}"
            name = "UNK"

        plddt = _residue_plddt(res)
        residues.append(Residue(
            index=idx,
            name=name,
            plddt=plddt,
            gemmi_residue=res,
        ))

    if not residues:
        return "no_residues"

    return ParseResult(
        residues=residues,
        chain=chain,
        model=model,
        structure=structure,
    )


def _residue_plddt(res: gemmi.Residue) -> float:
    """Compute per-residue pLDDT as mean B-factor of heavy atoms."""
    b_values = []
    for atom in res:
        if not atom.is_hydrogen():
            b_values.append(atom.b_iso)
    if not b_values:
        return float("-inf")
    return sum(b_values) / len(b_values)


def build_residue_index_map(parse_result: ParseResult) -> dict:
    """Build a map from gemmi Residue identity to 1-based index.

    Uses (subchain, residue_seqid_num, icode, residue_name) as key.
    """
    rmap = {}
    for r in parse_result.residues:
        gr = r.gemmi_residue
        key = _residue_key(gr)
        rmap[key] = r.index
    return rmap


def _residue_key(res: gemmi.Residue) -> tuple:
    """Unique key for a residue within a structure."""
    return (res.subchain, res.seqid.num, res.seqid.icode, res.name)
