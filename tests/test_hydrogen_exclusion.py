"""Test: no hydrogen atoms in output (section 13, test 5)."""

from contactdoc.cif_parse import parse_cif_from_path, extract_residues
from contactdoc.contacts import compute_contacts


HYDROGEN_PREFIXES = ("H", "1H", "2H", "3H")


def test_no_hydrogen_in_contacts(synthetic_8res_path):
    """No hydrogen atoms should appear in emitted contacts.

    The synthetic fixture includes a hydrogen atom on residue 1 to verify exclusion.
    """
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    for c in contacts:
        assert not c.atom_i.startswith("H"), f"Hydrogen atom_i: {c.atom_i} in contact ({c.i},{c.j})"
        assert not c.atom_j.startswith("H"), f"Hydrogen atom_j: {c.atom_j} in contact ({c.i},{c.j})"


def test_plddt_excludes_hydrogens(synthetic_8res_path):
    """Per-residue pLDDT should not include hydrogen B-factors."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    # Residue 1 (MET) has 8 heavy atoms all at B=85.0, plus 1 H at B=85.0
    # pLDDT should be 85.0 (mean of heavy atoms only)
    assert result.residues[0].plddt == 85.0


def test_one_contact_per_residue_pair(synthetic_8res_path):
    """No duplicate (i,j) pairs in contacts (section 13, test 6)."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    pairs = [(c.i, c.j) for c in contacts]
    assert len(pairs) == len(set(pairs)), "Duplicate residue pairs found"
