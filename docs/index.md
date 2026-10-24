# Big Data & Cloud Engineering Guide

**A production-grade reference** for data engineers working across Apache Spark, Delta Lake, Azure Databricks, AWS, and real-time streaming architectures.

!!! info "How to use this guide"
    Content is organized by **topic domain**, not by course week. Use the left navigation to explore any area, or use the **search bar** (top-right) to jump directly to a concept, API, or pattern.

---

## Modules

| # | Module | Topics |
|---|--------|--------|
| 01 | [Big Data Fundamentals](01-big-data-fundamentals/index.md) | 5Vs of Big Data, HDFS, MapReduce, distributed computing principles |
| 02 | [Apache Spark Core](02-apache-spark-core/index.md) | RDDs, DataFrames, SparkSQL, SparkSession, deployment modes |
| 03 | [Spark Performance](03-spark-performance/index.md) | Caching, partitioning, AQE, broadcast joins, memory management, file formats |
| 04 | [Production Engineering](04-production-engineering/index.md) | Resource sizing, testing, logging, Git, CI/CD, Apache Hive |
| 05 | [Delta Lake](05-delta-lake/index.md) | ACID transactions, time travel, Medallion architecture, Change Data Feed, optimization |
| 06 | [Azure Cloud & Databricks](06-azure-cloud/index.md) | Azure infrastructure, ADLS Gen2, Azure Databricks, Unity Catalog |
| 07 | [Pipeline Orchestration](07-pipeline-orchestration/index.md) | Azure Data Factory, Synapse Analytics, end-to-end pipeline design |
| 08 | [Data Modeling](08-data-modeling/index.md) | Normalization, dimensional modeling, star/snowflake schema, SCD Type-2 |
| 09 | [Streaming & Messaging](09-streaming-messaging/index.md) | Spark Structured Streaming, Apache Kafka, Kafka + Spark integration |
| 10 | [AWS Big Data](10-aws-big-data/index.md) | EMR, AWS Glue, S3, Lambda, Redshift, AWS data lake architecture |
| 11 | [Interview Preparation](11-interview-prep/index.md) | System design Q&A, technical question bank, DSA patterns |

---

## Technology Coverage

| Domain | Technologies |
|--------|-------------|
| Distributed Compute | Apache Spark (RDD, DataFrame, SparkSQL), Apache Hadoop (HDFS, MapReduce, YARN) |
| Storage Formats | Delta Lake, Parquet, ORC, Avro, CSV, JSON |
| Azure | Azure Databricks, ADLS Gen2, Azure Data Factory, Synapse Analytics, Unity Catalog |
| AWS | Amazon EMR, AWS Glue, Amazon S3, Amazon Redshift, AWS Lambda |
| Streaming | Spark Structured Streaming, Apache Kafka |
| DevOps | Git, GitHub Actions, Jenkins CI/CD |
| Data Modeling | Dimensional modeling, SCD Type-2 PySpark implementation |

---

## Where to Start

- **New to Big Data?** → [Big Data Fundamentals](01-big-data-fundamentals/index.md)
- **Working with Spark?** → [Apache Spark Core](02-apache-spark-core/index.md) → [Spark Performance](03-spark-performance/index.md)
- **Building on Azure Databricks?** → [Delta Lake](05-delta-lake/index.md) → [Azure Cloud & Databricks](06-azure-cloud/index.md)
- **Building on AWS?** → [AWS Big Data](10-aws-big-data/index.md)
- **Preparing for interviews?** → [Interview Preparation](11-interview-prep/index.md)
