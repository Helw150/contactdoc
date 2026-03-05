#!/usr/bin/env python3
"""CLI: load cluster TSV.GZ and print stats (or build a fast lookup)."""

import click

from contactdoc.clusters import load_cluster_mapping


@click.command()
@click.argument("tsv_gz_path")
def main(tsv_gz_path: str):
    click.echo(f"Loading cluster mapping from {tsv_gz_path}...")
    mapping = load_cluster_mapping(tsv_gz_path)
    reps = set(mapping.values())
    click.echo(f"Members: {len(mapping)}, Unique representatives: {len(reps)}")


if __name__ == "__main__":
    main()
