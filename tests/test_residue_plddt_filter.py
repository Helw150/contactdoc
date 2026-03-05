"""Test: per-residue pLDDT contact filtering (section 13, test 3)."""

from contactdoc.cif_parse import parse_cif_from_path, extract_residues
from contactdoc.contacts import compute_contacts, filter_contacts_by_plddt


def test_low_plddt_residue_contacts_filtered(low_plddt_path):
    """Contacts involving a residue with pLDDT < threshold must NOT be emitted."""
    st = parse_cif_from_path(low_plddt_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    # Residue 2 has pLDDT=50
    assert result.residues[1].plddt == 50.0

    contacts = compute_contacts(result, cutoff=4.0)
    filtered = filter_contacts_by_plddt(contacts, result, residue_plddt_min=70.0)

    # No contact should involve residue index 2 (pLDDT=50)
    for c in filtered:
        assert c.i != 2, f"Contact {c} has low-pLDDT residue i=2"
        assert c.j != 2, f"Contact {c} has low-pLDDT residue j=2"


def test_high_plddt_contacts_kept(synthetic_8res_path):
    """Contacts where both residues pass pLDDT threshold should be kept."""
    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, cutoff=4.0)
    # All residues have pLDDT >= 75.0, so with threshold 70.0 none should be filtered
    filtered = filter_contacts_by_plddt(contacts, result, residue_plddt_min=70.0)
    assert len(filtered) == len(contacts)
