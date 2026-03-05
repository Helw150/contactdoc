"""Contact computation using Gemmi NeighborSearch + ContactSearch."""

from dataclasses import dataclass

import gemmi

from .cif_parse import ParseResult, build_residue_index_map, _residue_key


@dataclass
class Contact:
    i: int  # 1-based residue index, i < j
    j: int
    atom_i: str  # atom name stripped
    atom_j: str
    distance: float


def compute_contacts(
    parse_result: ParseResult,
    cutoff: float = 4.0,
) -> list[Contact]:
    """Compute best contact per residue pair using Gemmi ContactSearch.

    Returns unsorted list of contacts (one per eligible residue pair).
    """
    model = parse_result.model
    structure = parse_result.structure

    ns = gemmi.NeighborSearch(model, structure.cell, cutoff)
    ns.populate(include_h=False)

    cs = gemmi.ContactSearch(cutoff)
    cs.ignore = gemmi.ContactSearch.Ignore.AdjacentResidues

    results = cs.find_contacts(ns)

    rmap = build_residue_index_map(parse_result)
    best: dict[tuple[int, int], tuple[float, str, str]] = {}

    for contact in results:
        cra1 = contact.partner1
        cra2 = contact.partner2

        key1 = _residue_key(cra1.residue)
        key2 = _residue_key(cra2.residue)

        idx1 = rmap.get(key1)
        idx2 = rmap.get(key2)
        if idx1 is None or idx2 is None:
            continue

        # Normalize so i < j
        if idx1 > idx2:
            idx1, idx2 = idx2, idx1
            cra1, cra2 = cra2, cra1

        if idx1 == idx2:
            continue
        # |i-j| > 1 should already be enforced by AdjacentResidues, but be safe
        if idx2 - idx1 <= 1:
            continue

        atom_name1 = cra1.atom.name.strip()
        atom_name2 = cra2.atom.name.strip()
        dist = contact.dist

        pair = (idx1, idx2)
        candidate = (dist, atom_name1, atom_name2)
        existing = best.get(pair)

        if existing is None:
            best[pair] = candidate
        else:
            # Smaller distance wins; tie-break by lex (atom_i, atom_j)
            if (candidate[0], candidate[1], candidate[2]) < (existing[0], existing[1], existing[2]):
                best[pair] = candidate

    return [
        Contact(i=i, j=j, atom_i=a1, atom_j=a2, distance=d)
        for (i, j), (d, a1, a2) in best.items()
    ]


def filter_contacts_by_plddt(
    contacts: list[Contact],
    parse_result: ParseResult,
    residue_plddt_min: float,
) -> list[Contact]:
    """Remove contacts where either endpoint residue has pLDDT below threshold."""
    plddt_map = {r.index: r.plddt for r in parse_result.residues}
    return [
        c for c in contacts
        if plddt_map.get(c.i, float("-inf")) >= residue_plddt_min
        and plddt_map.get(c.j, float("-inf")) >= residue_plddt_min
    ]


def sort_and_truncate(
    contacts: list[Contact],
    max_contacts: int,
) -> list[Contact]:
    """Sort by (j-i) desc, i asc, j asc. Truncate to max_contacts."""
    contacts.sort(key=lambda c: (-(c.j - c.i), c.i, c.j))
    return contacts[:max_contacts]
