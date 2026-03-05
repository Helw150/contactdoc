"""Test: no |i-j|<=1 contacts (section 13, test 4)."""

from contactdoc.cif_parse import parse_cif_from_path, extract_residues
from contactdoc.contacts import compute_contacts


def test_no_adjacent_contacts(synthetic_8res_path):
    """No emitted contact should have |i-j| <= 1."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    for c in contacts:
        assert abs(c.j - c.i) > 1, f"Adjacent contact found: ({c.i}, {c.j})"


def test_no_same_residue_contacts(synthetic_8res_path):
    """No contact should have i == j."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    for c in contacts:
        assert c.i != c.j, f"Same-residue contact found: ({c.i}, {c.j})"


def test_ascending_residue_order(synthetic_8res_path):
    """All contacts should have i < j."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    for c in contacts:
        assert c.i < c.j, f"Contact not in ascending order: ({c.i}, {c.j})"
