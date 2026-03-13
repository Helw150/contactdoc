"""I/O utilities: write sharded output as Parquet or gzipped text."""

import gzip
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


DOCUMENT_SCHEMA = pa.schema([
    ("document", pa.string()),
    ("entry_id", pa.string()),
    ("uniprot_accession", pa.string()),
    ("tax_id", pa.int64()),
    ("organism_name", pa.string()),
    ("global_plddt", pa.float32()),
    ("seq_len", pa.int32()),
    ("contacts_pre_filter", pa.int32()),
    ("contacts_emitted", pa.int32()),
    ("residues_passing_plddt", pa.int32()),
    ("split", pa.string()),
    ("seq_cluster_id", pa.string()),
    ("struct_cluster_id", pa.string()),
    ("split_cluster_id", pa.string()),
    ("sha1", pa.string()),
])

ERROR_SCHEMA = pa.schema([
    ("entry_id", pa.string()),
    ("gcs_uri", pa.string()),
    ("reason", pa.string()),
    ("exception", pa.string()),
    ("global_plddt", pa.float32()),
    ("seq_len", pa.int32()),
])


class ShardWriter:
    """Accumulates documents/errors per split, writes Parquet shard files."""

    def __init__(self, output_dir: str | Path, shard_index: int):
        self.output_dir = Path(output_dir)
        self.shard_index = shard_index
        self.shard_name = f"shard_{shard_index:06d}"
        # Buffers: split -> list of dicts
        self._doc_buffers: dict[str, list[dict]] = {}
        self._error_buffers: dict[str, list[dict]] = {}

    def add_document(self, split: str, doc_text: str, metadata: dict):
        self._doc_buffers.setdefault(split, []).append({
            "document": doc_text,
            "entry_id": metadata.get("entryId", ""),
            "uniprot_accession": metadata.get("uniprotAccession", ""),
            "tax_id": metadata.get("taxId", 0),
            "organism_name": metadata.get("organismScientificName", ""),
            "global_plddt": metadata.get("globalMetricValue_mean_pLDDT", 0.0),
            "seq_len": metadata.get("seq_len", 0),
            "contacts_pre_filter": metadata.get("contacts_found_pre_confidence_filter", 0),
            "contacts_emitted": metadata.get("contacts_emitted", 0),
            "residues_passing_plddt": metadata.get("residues_passing_plddt", 0),
            "split": metadata.get("split", ""),
            "seq_cluster_id": metadata.get("seq_cluster_id", ""),
            "struct_cluster_id": metadata.get("struct_cluster_id", ""),
            "split_cluster_id": metadata.get("split_cluster_id", ""),
            "sha1": metadata.get("sha1_of_document_text", ""),
        })

    def add_error(self, split: str, error: dict):
        self._error_buffers.setdefault(split, []).append({
            "entry_id": error.get("entryId", ""),
            "gcs_uri": error.get("gcs_uri", ""),
            "reason": error.get("reason", ""),
            "exception": error.get("exception") or "",
            "global_plddt": error.get("globalMetricValue") or 0.0,
            "seq_len": error.get("seq_len") or 0,
        })

    def flush(self) -> list[str]:
        """Write all buffers to disk as Parquet. Returns list of written file paths."""
        written = []
        all_splits = set(self._doc_buffers) | set(self._error_buffers)
        for split in sorted(all_splits):
            split_dir = self.output_dir / split
            split_dir.mkdir(parents=True, exist_ok=True)

            if split in self._doc_buffers and self._doc_buffers[split]:
                path = split_dir / f"{self.shard_name}.parquet"
                rows = self._doc_buffers[split]
                table = pa.table(
                    {col: [r[col] for r in rows] for col in DOCUMENT_SCHEMA.names},
                    schema=DOCUMENT_SCHEMA,
                )
                pq.write_table(table, path, compression="zstd")
                written.append(str(path))

            if split in self._error_buffers and self._error_buffers[split]:
                path = split_dir / f"{self.shard_name}.errors.parquet"
                rows = self._error_buffers[split]
                table = pa.table(
                    {col: [r[col] for r in rows] for col in ERROR_SCHEMA.names},
                    schema=ERROR_SCHEMA,
                )
                pq.write_table(table, path, compression="zstd")
                written.append(str(path))

        return written


def read_gz(path: str | Path) -> str:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return f.read()
