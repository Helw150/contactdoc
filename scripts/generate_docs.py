#!/usr/bin/env python3
"""CLI: generate ContactDoc documents from a Parquet shard (no GCS access needed).

Reads CIF content from the cif_content column, parses, computes contacts,
and writes the same sharded output format as process_manifest_shard.py.
"""

import traceback

import click
import pyarrow.parquet as pq

from contactdoc.cif_parse import extract_residues, parse_cif
from contactdoc.config import config_sha, load_config
from contactdoc.contacts import compute_contacts, filter_contacts_by_plddt, sort_and_truncate
from contactdoc.io import ShardWriter
from contactdoc.serialize import (
    make_error_record,
    make_metadata_record,
    metadata_to_jsonl,
    error_to_jsonl,
    serialize_document,
)


def process_row(row: dict, cfg) -> tuple[str | None, str | None, str | None, str]:
    """Process a single Parquet row.

    Returns (doc_text, metadata_jsonl, error_jsonl, split).
    """
    split = row["split"]
    entry_id = row["entry_id"]
    # Build an entry dict compatible with existing serialize helpers
    entry = {
        "entryId": entry_id,
        "uniprotAccession": row.get("uniprot_accession", ""),
        "taxId": row.get("tax_id", 0),
        "organismScientificName": row.get("organism_name", ""),
        "globalMetricValue": row.get("global_plddt", 0.0),
        "seq_len": row.get("seq_len", 0),
        "uniprotStart": 0,
        "uniprotEnd": 0,
        "latestVersion": 0,
        "gcs_uri": row.get("gcs_uri", ""),
        "split": split,
        "seq_cluster_id": row.get("seq_cluster_id", ""),
        "struct_cluster_id": row.get("struct_cluster_id", ""),
        "split_cluster_id": row.get("struct_cluster_id", ""),
    }

    try:
        structure = parse_cif(row["cif_content"])

        result = extract_residues(
            structure,
            require_single_chain=cfg.filters.require_single_chain,
            canonical_residue_policy=cfg.filters.canonical_residue_policy,
        )
        if isinstance(result, str):
            err = make_error_record(entry, result)
            return None, None, error_to_jsonl(err), split

        contacts = compute_contacts(result, cfg.contacts.cutoff_angstrom)
        contacts_pre_filter = len(contacts)

        contacts = filter_contacts_by_plddt(contacts, result, cfg.filters.residue_plddt_min)

        if not contacts:
            err = make_error_record(entry, "no_contacts_after_filter")
            return None, None, error_to_jsonl(err), split

        contacts = sort_and_truncate(contacts, cfg.contacts.max_contacts_per_doc)

        doc_text = serialize_document(result.residues, contacts)
        meta = make_metadata_record(
            entry, result, contacts_pre_filter, len(contacts), doc_text, cfg,
        )
        return doc_text, metadata_to_jsonl(meta), None, split

    except Exception:
        err = make_error_record(entry, "exception", traceback.format_exc())
        return None, None, error_to_jsonl(err), split


@click.command()
@click.option("--config", "config_path", required=True, help="Path to YAML config")
@click.option("--parquet-shard", required=True, help="Path to a Parquet shard file")
@click.option("--shard-index", required=True, type=int, help="Output shard index")
@click.option("--output-dir", required=True, help="Output directory for doc shards")
def main(config_path: str, parquet_shard: str, shard_index: int, output_dir: str):
    cfg = load_config(config_path)

    table = pq.read_table(parquet_shard)
    click.echo(f"Processing shard {shard_index} with {len(table)} entries from Parquet")

    writer = ShardWriter(output_dir, shard_index)
    success_count = 0
    error_count = 0

    for i in range(len(table)):
        row = {col: table.column(col)[i].as_py() for col in table.column_names}
        doc_text, meta_jsonl, err_jsonl, split = process_row(row, cfg)
        if doc_text is not None:
            writer.add_document(split, doc_text, meta_jsonl)
            success_count += 1
        if err_jsonl is not None:
            writer.add_error(split, err_jsonl)
            error_count += 1

    paths = writer.flush()
    click.echo(f"Done: {success_count} docs, {error_count} errors, {len(paths)} files written")


if __name__ == "__main__":
    main()
