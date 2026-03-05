"""Test: golden example format validation (section 13, test 11)."""

from contactdoc.contacts import Contact
from contactdoc.serialize import serialize_document
from contactdoc.cif_parse import Residue


def _make_residues(names):
    return [Residue(index=i+1, name=n, plddt=85.0, gemmi_residue=None) for i, n in enumerate(names)]


def test_golden_format():
    """Document matches the exact format from spec section 10."""
    residues = _make_residues(["MET", "LYS", "PHE", "CYS"])
    contacts = [
        Contact(i=1, j=4, atom_i="SD", atom_j="SG", distance=3.5),
        Contact(i=1, j=3, atom_i="CE", atom_j="CZ", distance=3.2),
    ]
    doc = serialize_document(residues, contacts)

    lines = doc.strip().split("\n")
    assert lines[0] == "<begin_sequence>"
    assert lines[1] == "<MET> <LYS> <PHE> <CYS>"
    assert lines[2] == "<begin_contacts>"
    assert lines[3] == "<p1> <p4> <SD> <SG>"
    assert lines[4] == "<p1> <p3> <CE> <CZ>"
    assert lines[5] == "<end_contacts>"
    assert lines[6] == "<end>"


def test_contact_line_has_4_tokens():
    """Each contact line must have exactly 4 tokens."""
    residues = _make_residues(["ALA", "GLY", "LEU"])
    contacts = [Contact(i=1, j=3, atom_i="CA", atom_j="CB", distance=3.0)]
    doc = serialize_document(residues, contacts)

    for line in doc.strip().split("\n"):
        if line.startswith("<p"):
            tokens = line.split()
            assert len(tokens) == 4, f"Contact line has {len(tokens)} tokens: {line}"


def test_markers_exact():
    """Begin/end markers are exactly as specified."""
    residues = _make_residues(["ALA"])
    doc = serialize_document(residues, [])
    lines = doc.strip().split("\n")
    assert lines[0] == "<begin_sequence>"
    assert lines[2] == "<begin_contacts>"
    assert lines[3] == "<end_contacts>"
    assert lines[4] == "<end>"


def test_empty_contacts():
    """A doc with no contacts should still have markers."""
    residues = _make_residues(["ALA", "GLY"])
    doc = serialize_document(residues, [])
    assert "<begin_contacts>" in doc
    assert "<end_contacts>" in doc
    assert "<end>" in doc


def test_unk_residue():
    """UNK residues serialize as <UNK>."""
    residues = _make_residues(["ALA", "UNK", "GLY"])
    doc = serialize_document(residues, [])
    assert "<UNK>" in doc
