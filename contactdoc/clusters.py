"""Load cluster mapping files (TSV.GZ): member_id -> rep_id."""

import gzip
from pathlib import Path


def load_cluster_mapping(tsv_gz_path: str | Path) -> dict[str, str]:
    """Load a Foldseek-style TSV.GZ cluster file.

    Format: rep_id<TAB>member_id per line.
    Returns dict mapping member_id -> rep_id.
    """
    mapping: dict[str, str] = {}
    with gzip.open(tsv_gz_path, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            rep_id, member_id = parts
            mapping[member_id] = rep_id
    return mapping


def get_cluster_id(entry_id: str, cluster_map: dict[str, str] | None) -> str:
    """Get cluster rep ID for an entry. Falls back to entry_id if not found."""
    if cluster_map is None:
        return entry_id
    return cluster_map.get(entry_id, entry_id)
