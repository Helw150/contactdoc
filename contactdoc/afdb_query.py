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
    uniprotSequence,
    LENGTH(uniprotSequence) AS seq_len
FROM `{cfg.bigquery_table}`
WHERE {where}
ORDER BY entryId{limit_clause}
"""


def run_selection_query(cfg: PipelineConfig, limit: int | None = None) -> list[dict]:
    """Execute the selection query and return rows as list of dicts."""
    from google.cloud import bigquery

    client = bigquery.Client()
    query = build_selection_query(cfg, limit=limit)
    rows = client.query(query).result()
    return [dict(row) for row in rows]
