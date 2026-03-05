"""Test: contact sort order + truncation (section 13, tests 8-9)."""

from contactdoc.contacts import Contact, sort_and_truncate


def test_sort_order():
    """Contacts sorted by (j-i) desc, i asc, j asc."""
    contacts = [
        Contact(i=1, j=3, atom_i="CA", atom_j="CB", distance=3.0),  # sep=2
        Contact(i=1, j=8, atom_i="O",  atom_j="O",  distance=2.0),  # sep=7
        Contact(i=2, j=5, atom_i="NZ", atom_j="OD1", distance=3.5), # sep=3
        Contact(i=3, j=5, atom_i="C",  atom_j="N",  distance=3.3),  # sep=2
        Contact(i=2, j=8, atom_i="CD", atom_j="N",  distance=1.9),  # sep=6
    ]
    sorted_c = sort_and_truncate(contacts, 2048)

    seps = [(c.j - c.i, c.i, c.j) for c in sorted_c]
    # Should be: (7,1,8), (6,2,8), (3,2,5), (2,1,3), (2,3,5)
    assert seps == [(7, 1, 8), (6, 2, 8), (3, 2, 5), (2, 1, 3), (2, 3, 5)]


def test_truncation():
    """Truncation takes the prefix of the sorted list."""
    contacts = [
        Contact(i=1, j=8, atom_i="O",  atom_j="O",  distance=2.0),  # sep=7
        Contact(i=2, j=8, atom_i="CD", atom_j="N",  distance=1.9),  # sep=6
        Contact(i=2, j=7, atom_i="NZ", atom_j="N",  distance=1.0),  # sep=5
        Contact(i=1, j=3, atom_i="CA", atom_j="CB", distance=3.0),  # sep=2
    ]
    sorted_c = sort_and_truncate(contacts, 2)
    assert len(sorted_c) == 2
    assert sorted_c[0].i == 1 and sorted_c[0].j == 8  # sep=7, first
    assert sorted_c[1].i == 2 and sorted_c[1].j == 8  # sep=6, second


def test_truncation_no_excess():
    """If contacts <= max, no truncation."""
    contacts = [Contact(i=1, j=5, atom_i="CA", atom_j="CB", distance=3.0)]
    sorted_c = sort_and_truncate(contacts, 2048)
    assert len(sorted_c) == 1


def test_ordering_on_real_fixture(synthetic_8res_path):
    """Verify sort order on real parsed contacts."""
    from contactdoc.cif_parse import parse_cif_from_path, extract_residues
    from contactdoc.contacts import compute_contacts, filter_contacts_by_plddt

    st = parse_cif_from_path(synthetic_8res_path)
    result = extract_residues(st)
    assert not isinstance(result, str)

    contacts = compute_contacts(result, 4.0)
    contacts = filter_contacts_by_plddt(contacts, result, 70.0)
    sorted_c = sort_and_truncate(contacts, 2048)

    # Verify sort invariant
    for k in range(len(sorted_c) - 1):
        a, b = sorted_c[k], sorted_c[k + 1]
        sep_a = a.j - a.i
        sep_b = b.j - b.i
        assert (sep_a, -a.i, -a.j) >= (sep_b, -b.i, -b.j), \
            f"Sort violated at {k}: ({a.i},{a.j}) sep={sep_a} vs ({b.i},{b.j}) sep={sep_b}"
