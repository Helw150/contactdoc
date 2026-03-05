import pytest
from pathlib import Path

from contactdoc.config import PipelineConfig, FiltersConfig, ContactsConfig, SplitsConfig


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixture_dir():
    return FIXTURES_DIR


@pytest.fixture
def synthetic_8res_path():
    return str(FIXTURES_DIR / "synthetic_8res.cif")


@pytest.fixture
def low_plddt_path():
    return str(FIXTURES_DIR / "synthetic_low_plddt.cif")


@pytest.fixture
def noncanonical_path():
    return str(FIXTURES_DIR / "synthetic_noncanonical.cif")


@pytest.fixture
def default_config():
    return PipelineConfig(
        filters=FiltersConfig(
            skip_fragments=True,
            global_mean_plddt_min=70.0,
            residue_plddt_min=70.0,
            max_seq_len=2048,
            require_single_chain=True,
            canonical_residue_policy="map_to_unk",
        ),
        contacts=ContactsConfig(
            cutoff_angstrom=4.0,
            max_contacts_per_doc=2048,
        ),
        splits=SplitsConfig(
            seed="contactdoc-v1",
            train_frac=0.98,
            val_frac=0.01,
            test_frac=0.01,
        ),
    )
