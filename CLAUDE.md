# CLAUDE.md

This is a MkDocs documentation site for **Big Data & Cloud Engineering Guide** — a production-grade reference covering Big Data fundamentals, Apache Spark, Delta Lake, Azure, AWS, and streaming architectures.

## Setup

```bash
pip install -r requirements.txt
```

## Common Commands

| Task | Command |
|------|---------|
| Local preview | `mkdocs serve` → http://127.0.0.1:8000/ |
| Build static site | `mkdocs build` |
| Deploy to GitHub Pages | Push to `main` — GitHub Actions handles it |

## Architecture

### Theme

Uses `mkdocs-material` with deep-purple color scheme.

### Navigation

All navigation is declared in `mkdocs.yml` under `nav:`. Adding a new page requires both creating the `.md` file and adding a matching entry there.

### Documentation Structure

Pages are organized by topic domain under `docs/`:
- `docs/01-big-data-fundamentals/` — 5Vs, HDFS, MapReduce, distributed systems
- `docs/02-apache-spark-core/` — RDDs, DataFrames, SparkSQL, SparkSession
- `docs/03-spark-performance/` — Caching, partitioning, AQE, memory management
- `docs/04-production-engineering/` — Resource sizing, testing, logging, CI/CD, Hive
- `docs/05-delta-lake/` — Delta Lake, Medallion architecture, CDF, optimization
- `docs/06-azure-cloud/` — Azure infra, ADLS Gen2, Azure Databricks, Unity Catalog
- `docs/07-pipeline-orchestration/` — ADF, Synapse, end-to-end pipeline
- `docs/08-data-modeling/` — Normalization, dimensional modeling, SCDs
- `docs/09-streaming-messaging/` — Spark Structured Streaming, Kafka
- `docs/10-aws-big-data/` — EMR, Glue, S3, Lambda, Redshift
- `docs/11-interview-prep/` — System design, Q&A bank, DSA

### Diagrams

Mermaid diagrams supported via `mkdocs-mermaid2-plugin`.

## Page Conventions

- Every `.md` file starts with a single `#` heading.
- Use MkDocs Material tabs: `=== "PySpark"` / `=== "SQL"` for dual-API content.
- Use admonitions: `!!! tip`, `!!! warning`, `!!! info`, `!!! note`.
- No week numbers in headings — content is organized by concept domain.
- No version/history tables — Git history is source of truth.
