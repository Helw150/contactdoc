"""Manifest building: enrich entries with cluster_id + split, write shards."""

import json
from pathlib import Path

from .clusters import get_cluster_id
from .config import PipelineConfig
from .splits import assign_split


def enrich_entry(
    entry: dict,
    seq_cluster_map: dict[str, str],
    struct_cluster_map: dict[str, str],
    cfg: PipelineConfig,
) -> dict | None:
    """Attach cluster IDs, split, and gcs_uri to a single entry.

    Returns enriched entry dict, or None if the entry is missing from
    either cluster map.
    """
    entry_id = entry["entryId"]
    accession = entry["uniprotAccession"]
    seq_cluster_id = get_cluster_id(accession, seq_cluster_map)
    struct_cluster_id = get_cluster_id(accession, struct_cluster_map)
    if seq_cluster_id is None or struct_cluster_id is None:
        return None
    split = assign_split(
        cfg.splits.seed,
        struct_cluster_id,
        cfg.splits.train_frac,
        cfg.splits.val_frac,
    )
    gcs_uri = f"{cfg.gcs_bucket_prefix}{entry_id}-model_v{cfg.afdb_version}.cif"

    return {
        **entry,
        "seq_cluster_id": seq_cluster_id,
        "struct_cluster_id": struct_cluster_id,
        "split_cluster_id": struct_cluster_id,
        "split": split,
        "gcs_uri": gcs_uri,
    }


class StreamingManifestWriter:
    """Writes manifest shards on-the-fly as entries stream in."""

    def __init__(self, output_dir: str | Path, shard_size: int):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.shard_size = shard_size
        self._buffer: list[dict] = []
        self._shard_num = 0
        self._paths: list[str] = []

    def add(self, entry: dict):
        self._buffer.append(entry)
        if len(self._buffer) >= self.shard_size:
            self._flush_buffer()

    def finish(self) -> list[str]:
        if self._buffer:
            self._flush_buffer()
        return self._paths

    def _flush_buffer(self):
        path = self.output_dir / f"manifest_shard_{self._shard_num:06d}.jsonl"
        with open(path, "w") as f:
            for entry in self._buffer:
                row = {k: v for k, v in entry.items() if k != "uniprotSequence"}
                f.write(json.dumps(row, ensure_ascii=True) + "\n")
        self._paths.append(str(path))
        self._shard_num += 1
        self._buffer = []


def read_manifest_shard(path: str | Path) -> list[dict]:
    """Read a manifest shard JSONL file."""
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries
