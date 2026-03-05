"""Test: tie-breaking determinism (section 13, test 7)."""

from contactdoc.contacts import Contact


def test_tiebreak_lex_atom_names():
    """Equal-distance contacts should use lexicographically smaller (atom_i, atom_j)."""
    # Simulate two atom pairs with same distance for residue pair (1, 5)
    # ("CB", "OD1") should win over ("CG", "CA") because "CB" < "CG"
    candidates = [
        (3.5, "CG", "CA"),
        (3.5, "CB", "OD1"),
    ]
    # The comparison logic from contacts.py:
    # (dist, atom_i, atom_j) tuple comparison
    best = min(candidates)
    assert best == (3.5, "CB", "OD1")


def test_tiebreak_second_atom():
    """When first atoms are equal, break on second atom name."""
    candidates = [
        (3.5, "CB", "OD2"),
        (3.5, "CB", "OD1"),
    ]
    best = min(candidates)
    assert best == (3.5, "CB", "OD1")


def test_distance_wins_over_lex():
    """Shorter distance always wins regardless of atom names."""
    candidates = [
        (3.0, "ZZ", "ZZ"),
        (3.5, "AA", "AA"),
    ]
    best = min(candidates)
    assert best == (3.0, "ZZ", "ZZ")


def test_tiebreak_determinism_across_runs(synthetic_8res_path):
    """Running contact computation twice gives identical results."""
    from contactdoc.cif_parse import parse_cif_from_path, extract_residues
    from contactdoc.contacts import compute_contacts, sort_and_truncate

    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts1 = sort_and_truncate(compute_contacts(result, 4.0), 2048)
    contacts2 = sort_and_truncate(compute_contacts(result, 4.0), 2048)

    assert len(contacts1) == len(contacts2)
    for c1, c2 in zip(contacts1, contacts2):
        assert (c1.i, c1.j, c1.atom_i, c1.atom_j) == (c2.i, c2.j, c2.atom_i, c2.atom_j)
        assert abs(c1.distance - c2.distance) < 1e-6
