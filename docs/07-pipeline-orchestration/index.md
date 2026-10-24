# Pipeline Orchestration

Modern enterprise data platforms separate concerns across three layers: **orchestration**, **compute**, and **storage**. This module covers how Azure's native services snap together into a production data pipeline — Azure Data Factory (ADF) driving the workflow, Databricks and Synapse performing the heavy computation, and ADLS Gen2 providing the durable, cost-effective storage beneath everything.

---

## The Orchestration Landscape

```mermaid
graph TD
    subgraph Sources["Source Systems"]
        SQL["Azure SQL / On-Prem DB"]
        S3["AWS S3"]
        HTTP["HTTP / REST APIs"]
        Files["File Drops (CSV, JSON, Parquet)"]
    end

    subgraph ADF["Azure Data Factory — Orchestrator"]
        Trigger["Triggers\n(Schedule / Event / Tumbling Window)"]
        Pipeline["Pipelines & Activities"]
        IR["Integration Runtime\n(Azure / SHIR / SSIS)"]
    end

    subgraph Compute["Compute Engines"]
        Databricks["Azure Databricks\n(Spark, Delta Lake, ML)"]
        Synapse["Azure Synapse\n(Dedicated SQL Pool / Spark Pool)"]
    end

    subgraph Storage["ADLS Gen2 — Storage Layer"]
        Landing["landing/\n(raw ingest)"]
        Bronze["Bronze\n(raw Delta)"]
        Silver["Silver\n(cleansed Delta)"]
        Gold["Gold\n(aggregated Delta)"]
    end

    subgraph Serve["Serving & Consumption"]
        DBSQL["Databricks SQL\n(Lakeview / Genie)"]
        SynapseServe["Synapse Serverless SQL\n(ad-hoc T-SQL)"]
        PowerBI["Power BI\n(DirectQuery / Import)"]
    end

    Sources --> ADF
    ADF --> Storage
    Storage --> Databricks
    Storage --> Synapse
    Databricks --> Storage
    Synapse --> Storage
    Storage --> Serve
    Serve --> PowerBI
```

---

## Core Services at a Glance

| Service | Role | When to Reach For It |
|---|---|---|
| **Azure Data Factory** | Orchestrator — schedules, triggers, moves data, calls compute | Connecting 100+ sources, metadata-driven ETL, event-driven ingestion |
| **Azure Databricks** | Compute — Spark, Delta Lake, ML, streaming | Python-heavy ETL, Delta transformations, ML pipelines, complex joins |
| **Azure Synapse Dedicated SQL** | Compute — MPP SQL data warehouse | Structured DW workloads, complex SQL, BI reporting at scale |
| **Azure Synapse Serverless SQL** | Compute — query-in-place on Data Lake | Ad-hoc exploration, ETL result verification, pay-per-query |
| **ADLS Gen2** | Storage — hierarchical namespace object store | All raw and processed data; bronze/silver/gold Medallion layers |
| **Azure Key Vault** | Security — secret store | All credentials, connection strings, storage keys |

!!! info "ADF is the conductor, not the instrument"
    ADF's job is to coordinate — it moves data between systems and triggers compute jobs. It does not replace Apache Spark for complex transformations. When you find yourself writing business logic inside ADF Mapping Data Flows, that logic probably belongs in a Databricks notebook.

---

## The Medallion Pattern in Azure

The Medallion Architecture (Bronze → Silver → Gold) is the standard layering convention for Delta Lake on ADLS Gen2. ADF handles ingestion into the landing zone; Databricks promotes data through the layers.

```mermaid
flowchart LR
    Raw["Source Systems\n(SQL, S3, APIs, Files)"]
    Land["ADLS Gen2\nlanding/\n(raw files)"]
    Bronze["ADLS Gen2\nbronze/\n(raw Delta, schema preserved)"]
    Silver["ADLS Gen2\nsilver/\n(cleansed, deduplicated, typed)"]
    Gold["ADLS Gen2\ngold/\n(aggregated, business-ready)"]
    Serve["Serving Layer\n(DBSQL / Synapse / Power BI)"]

    Raw -->|"ADF Copy Activity"| Land
    Land -->|"Databricks Auto Loader"| Bronze
    Bronze -->|"Databricks ETL Job"| Silver
    Silver -->|"Databricks Agg Job"| Gold
    Gold --> Serve
```

| Layer | Format | Transformation | Owner |
|---|---|---|---|
| **Landing** | Native (CSV, JSON, Parquet) | None — raw as-delivered | ADF |
| **Bronze** | Delta Lake | Schema enforcement, append-only | Databricks |
| **Silver** | Delta Lake | Dedup, type casting, NULL handling, joins | Databricks |
| **Gold** | Delta Lake | Business aggregations, KPIs, denormalized facts | Databricks |

---

## Event-Driven vs. Scheduled Ingestion

Two triggering patterns dominate production pipelines:

```mermaid
flowchart TD
    subgraph EventDriven["Event-Driven (Reactive)"]
        FileArrival["Vendor drops file\nto ADLS landing/"]
        EventTrigger["Storage Event Trigger\n(Blob Created)"]
        ADFPipeline1["ADF Pipeline fires\nwithin seconds"]
        FileArrival --> EventTrigger --> ADFPipeline1
    end

    subgraph Scheduled["Scheduled (Proactive)"]
        Clock["Cron expression\n(e.g. 02:00 UTC daily)"]
        SchedTrigger["Schedule Trigger\nor Tumbling Window"]
        ADFPipeline2["ADF Pipeline fires\non schedule"]
        Clock --> SchedTrigger --> ADFPipeline2
    end
```

!!! tip "Prefer event-driven when latency matters"
    Storage Event Triggers react within seconds of a file landing. Schedule Triggers are simpler to reason about but introduce up to one full schedule-interval of latency. Use Tumbling Window Triggers when you need both scheduling *and* the ability to backfill missed historical windows.

---

## Security Architecture

No credential should ever appear as plaintext inside a pipeline definition, notebook, or configuration file. The pattern is:

```mermaid
flowchart LR
    Secret["Azure Key Vault\n(passwords, storage keys, tokens)"]
    ADF_LS["ADF Linked Service\n(references KV secret by name)"]
    DB_Scope["Databricks Secret Scope\n(backed by Key Vault)"]
    Notebook["Databricks Notebook\ndbutils.secrets.get(scope, key)"]
    Pipeline["ADF Pipeline\n@linkedService().password"]

    Secret --> ADF_LS --> Pipeline
    Secret --> DB_Scope --> Notebook
```

---

## Module Contents

| Page | What You Will Learn |
|---|---|
| [Azure Data Factory](adf.md) | ADF architecture, all activity types, triggers, parameters, Key Vault integration, metadata-driven patterns |
| [Azure Synapse Analytics](synapse.md) | Dedicated SQL Pool (MPP), Serverless SQL Pool, Spark Pool, distribution strategies, COPY INTO, CTAS |
| [End-to-End Pipeline Design](e2e-pipeline.md) | Full reference architecture, idempotency, validation gates, error handling, audit logging, master orchestrator pattern |
