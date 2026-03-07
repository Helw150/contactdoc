"""BigQuery selection query for AFDB entries (section 4.1)."""

from .config import PipelineConfig


def build_selection_query(cfg: PipelineConfig, limit: int | None = None) -> str:
    """Build BigQuery SQL to select AFDB entries matching config filters."""
    conditions = [
        f"latestVersion = {cfg.afdb_version}",
        f"globalMetricValue >= {cfg.filters.global_mean_plddt_min}",
        f"LENGTH(uniprotSequence) <= {cfg.filters.max_seq_len}",
    ]

    if cfg.filters.skip_fragments:
        conditions.append("uniprotStart = 1")
        conditions.append("uniprotEnd = LENGTH(uniprotSequence)")

    where = " AND ".join(conditions)
    limit_clause = f"\nLIMIT {limit}" if limit else ""

    return f"""
SELECT
    entryId,
    uniprotAccession,
    taxId,
    organismScientificName,
    latestVersion,
    globalMetricValue,
    uniprotStart,
    uniprotEnd,
    LENGTH(uniprotSequence) AS seq_len
FROM `{cfg.bigquery_table}`
WHERE {where}{limit_clause}
"""


def run_selection_query(cfg: PipelineConfig, limit: int | None = None):
    """Execute the selection query and return a row iterator.

    Returns an iterator of Row objects (dict-like) to avoid loading
    all results into memory at once.
    """
    from google.cloud import bigquery

    client = bigquery.Client()
    query = build_selection_query(cfg, limit=limit)
    return client.query(query).result()
