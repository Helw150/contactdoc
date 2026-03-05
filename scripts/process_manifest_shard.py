#!/usr/bin/env python3
"""CLI: process one manifest shard -> output shards (text.gz, metadata.jsonl.gz, errors.jsonl.gz)."""

import traceback

import click

from contactdoc.cif_parse import extract_residues, parse_cif, parse_cif_from_path
from contactdoc.config import config_sha, load_config
from contactdoc.contacts import compute_contacts, filter_contacts_by_plddt, sort_and_truncate
from contactdoc.io import ShardWriter
from contactdoc.manifest import read_manifest_shard
from contactdoc.serialize import (
    make_error_record,
    make_metadata_record,
    metadata_to_jsonl,
    error_to_jsonl,
    serialize_document,
)


def process_entry(entry: dict, cfg, use_gcs: bool = False) -> tuple[str | None, str | None, str | None, str]:
    """Process a single manifest entry.

    Returns (doc_text, metadata_jsonl, error_jsonl, split).
    doc_text and metadata_jsonl are None on skip/error.
    error_jsonl is None on success.
    """
    split = entry["split"]
    entry_id = entry["entryId"]

    try:
        # Download/read CIF
        if use_gcs:
            cif_content = _download_from_gcs(entry["gcs_uri"])
            structure = parse_cif(cif_content)
        else:
            # Local mode: expect gcs_uri to be a local path or use a local cache
            structure = parse_cif_from_path(entry["gcs_uri"])

        # Extract residues
        result = extract_residues(
            structure,
            require_single_chain=cfg.filters.require_single_chain,
            canonical_residue_policy=cfg.filters.canonical_residue_policy,
        )
        if isinstance(result, str):
            err = make_error_record(entry, result)
            return None, None, error_to_jsonl(err), split

        # Compute contacts
        contacts = compute_contacts(result, cfg.contacts.cutoff_angstrom)
        contacts_pre_filter = len(contacts)

        # pLDDT filter
        contacts = filter_contacts_by_plddt(contacts, result, cfg.filters.residue_plddt_min)

        # Skip if no contacts remain
        if not contacts:
            err = make_error_record(entry, "no_contacts_after_filter")
            return None, None, error_to_jsonl(err), split

        # Sort and truncate
        contacts = sort_and_truncate(contacts, cfg.contacts.max_contacts_per_doc)

        # Serialize
        doc_text = serialize_document(result.residues, contacts)
        meta = make_metadata_record(
            entry, result, contacts_pre_filter, len(contacts), doc_text, cfg,
        )
        return doc_text, metadata_to_jsonl(meta), None, split

    except Exception as e:
        err = make_error_record(entry, "exception", traceback.format_exc())
        return None, None, error_to_jsonl(err), split


_gcs_client = None


def _get_gcs_client():
    global _gcs_client
    if _gcs_client is None:
        from google.cloud import storage
        _gcs_client = storage.Client()
    return _gcs_client


def _download_from_gcs(gcs_uri: str) -> str:
    """Download a CIF file from GCS and return its content as string.

    Requires authenticated GCP credentials — see SETUP.md.
    The AFDB v4 bucket is public but requester-pays, so a billing project is needed.
    """
    assert gcs_uri.startswith("gs://")
    parts = gcs_uri[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]

    client = _get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()


@click.command()
@click.option("--config", "config_path", required=True, help="Path to YAML config")
@click.option("--manifest-shard", required=True, help="Path to manifest shard JSONL")
@click.option("--shard-index", required=True, type=int, help="Output shard index")
@click.option("--output-dir", default=None, help="Override output directory")
@click.option("--use-gcs/--local", default=False, help="Download from GCS or use local paths")
def main(config_path: str, manifest_shard: str, shard_index: int, output_dir: str | None, use_gcs: bool):
    cfg = load_config(config_path)
    sha = config_sha(cfg)

    if output_dir is None:
        output_dir = f"{cfg.output_prefix}config_sha={sha}"

    entries = read_manifest_shard(manifest_shard)
    click.echo(f"Processing shard {shard_index} with {len(entries)} entries")

    writer = ShardWriter(output_dir, shard_index)
    success_count = 0
    error_count = 0

    for entry in entries:
        doc_text, meta_jsonl, err_jsonl, split = process_entry(entry, cfg, use_gcs)
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
