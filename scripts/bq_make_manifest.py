#!/usr/bin/env python3
"""CLI: config -> BigQuery -> manifest shards."""

import click

from contactdoc.afdb_query import run_selection_query
from contactdoc.clusters import load_cluster_mapping
from contactdoc.config import config_sha, load_config
from contactdoc.manifest import enrich_entries, write_manifest_shards


@click.command()
@click.option("--config", "config_path", required=True, help="Path to YAML config")
@click.option("--output-dir", default=None, help="Override manifest output directory")
@click.option("--limit", default=None, type=int, help="Limit BigQuery results (useful for smoke tests)")
def main(config_path: str, output_dir: str | None, limit: int | None):
    cfg = load_config(config_path)
    sha = config_sha(cfg)
    click.echo(f"Config SHA: {sha}")

    # Load cluster mapping if configured
    cluster_map = None
    cluster_path = getattr(cfg.cluster_files, f"{cfg.splits.mode}_rep_mem_tsv_gz", "")
    if cluster_path:
        click.echo(f"Loading cluster mapping from {cluster_path}...")
        cluster_map = load_cluster_mapping(cluster_path)
        click.echo(f"Loaded {len(cluster_map)} cluster members")

    # Run BigQuery selection
    click.echo("Running BigQuery selection query...")
    entries = run_selection_query(cfg, limit=limit)
    click.echo(f"Selected {len(entries)} entries")

    # Enrich with cluster + split
    enriched = enrich_entries(entries, cluster_map, cfg)

    # Write manifest shards
    if output_dir is None:
        output_dir = f"{cfg.output_prefix}config_sha={sha}/manifests"
    paths = write_manifest_shards(enriched, output_dir, cfg.parallelism.shard_size_entries)
    click.echo(f"Wrote {len(paths)} manifest shards to {output_dir}")


if __name__ == "__main__":
    main()
