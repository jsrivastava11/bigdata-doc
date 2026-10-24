# Delta Lake Optimization & Maintenance

Delta tables are self-describing and transactionally safe by default, but they do not
self-optimise. Without deliberate maintenance, high-write tables accumulate thousands of
small Parquet files, queries slow down as they scan more files than necessary, and old
unreferenced files consume storage indefinitely.

This page covers every performance and maintenance lever available in Delta Lake, from
the fundamentals (OPTIMIZE, VACUUM) through to Databricks-native accelerators (Photon,
Liquid Clustering, auto-optimize).

---

## The Small-File Problem

Every streaming micro-batch, every partial-partition write, and every UPDATE or DELETE
rewrites Parquet files. Over time this produces:

```
orders/ (after 1 week of streaming writes)
├── part-00000.snappy.parquet   4 KB
├── part-00001.snappy.parquet   7 KB
├── part-00002.snappy.parquet   3 KB
├── part-00003.snappy.parquet   12 KB
...
├── part-08541.snappy.parquet   5 KB    ← 8,000+ files
```

The impact is severe:

- Each file requires an individual object-storage API call (S3/ADLS list + open)
- Spark creates one task per file, overwhelming the driver with task scheduling overhead
- File-level statistics stored in the transaction log become unwieldy
- Query wall-clock time increases by 5–20× compared to a well-compacted table

The fix is **OPTIMIZE**.

---

## OPTIMIZE — File Compaction

`OPTIMIZE` merges small Parquet files into larger, query-friendly files. The default target
file size is **1 GB** (1024 MB). Files already larger than the target are left untouched.

=== "PySpark"

    ```python
    from delta.tables import DeltaTable

    dt = DeltaTable.forName(spark, "retaildb.orders")
    dt.optimize().executeCompaction()
    ```

=== "SQL"

    ```sql
    OPTIMIZE retaildb.orders;
    ```

After OPTIMIZE, the transaction log records `remove` entries for the small files and
`add` entries for the new consolidated files. Physically, the old small files remain on
disk until `VACUUM` removes them.

### When to Run OPTIMIZE

| Trigger | Reason |
|---|---|
| After streaming writes accumulate overnight | Micro-batches write many small files |
| After a large DELETE or UPDATE | Copy-on-write rewrites produce many medium-sized files |
| Before a heavy MERGE on Silver | Large input + small target files = slow join |
| Weekly on any high-write table | General hygiene |

!!! info "OPTIMIZE is idempotent"
    Running OPTIMIZE multiple times is safe. It skips files already at or above the target
    size. Use it freely in scheduled jobs.

---

## Z-ORDER — Multi-Dimensional Clustering

OPTIMIZE alone compacts files but does not control which rows end up in which files.
**Z-ORDER** combines compaction with data co-location: rows that share common values in
the specified columns are placed in the same physical files, enabling the query engine to
skip entire files based on column statistics.

=== "PySpark"

    ```python
    dt = DeltaTable.forName(spark, "retaildb.orders")
    dt.optimize().executeZOrderBy("customer_id", "order_date")
    ```

=== "SQL"

    ```sql
    OPTIMIZE retaildb.orders ZORDER BY (customer_id, order_date);
    ```

### How Data Skipping Uses Z-ORDER

Every `add` action in the Delta transaction log stores per-file column statistics (min
value, max value, null count). When a query has a filter predicate, Spark inspects these
stats before opening any Parquet file:

```
Query: WHERE customer_id = 4201 AND order_date = '2024-03-15'

File A: customer_id min=1,    max=1000   → SKIP (4201 not in range)
File B: customer_id min=1001, max=5000   → READ (4201 could be here)
File C: customer_id min=5001, max=9999   → SKIP (4201 not in range)
```

Without Z-ORDER, customer IDs are randomly distributed across files and most files must
be opened. With Z-ORDER, related customer IDs are clustered together and Spark skips
irrelevant files entirely — often reading less than 1% of the data for a selective query.

### Z-ORDER Best Practices

```sql
-- Good: frequently-filtered join key + date column
OPTIMIZE retaildb.orders ZORDER BY (customer_id, order_date);

-- Good: Silver layer — MERGE join key
OPTIMIZE retaildb.orders_silver ZORDER BY (order_id);

-- Avoid: more than 3-4 columns degrades effectiveness
-- (Z-curve loses multi-dimensional locality with too many dimensions)
OPTIMIZE retaildb.orders ZORDER BY (a, b, c, d, e);  -- too many
```

!!! note "OPTIMIZE + ZORDER together"
    `ZORDER BY` is always specified as an option to `OPTIMIZE` — it implicitly compacts
    files as well as clusters them. You do not need a separate `OPTIMIZE` without ZORDER
    first. The two operations are one command.

---

## Data Skipping

Data skipping is automatic — it requires no explicit configuration beyond having a Delta
table with populated statistics. When Delta writes a file, it records in the `add` action:

- `numRecords` — row count
- `minValues` — minimum value per column (first 32 columns by default)
- `maxValues` — maximum value per column
- `nullCount` — number of nulls per column

At query time, the Delta engine reads these stats from the transaction log (in memory) and
constructs a **file selection predicate** before issuing any Parquet read. Files whose
min/max range cannot possibly contain qualifying rows are skipped entirely.

```
Query: WHERE total_amount BETWEEN 500 AND 600

Transaction log stats (read from _delta_log, no Parquet I/O):
  File A: min_total=10,  max_total=499   → SKIP
  File B: min_total=450, max_total=800   → READ
  File C: min_total=601, max_total=9999  → SKIP
  File D: min_total=100, max_total=2000  → READ

Only 2 of 4 files read. 50% I/O saved.
```

Z-ORDER amplifies data skipping by making the min/max ranges narrower and more selective
per file — a file touched by Z-ORDER on `customer_id` will contain a much tighter range
of customer IDs than a randomly written file.

---

## VACUUM — Retention Cleanup

Delta's copy-on-write model means that old Parquet files are never deleted immediately —
they are only logically removed (via `remove` actions in the transaction log). Physical
deletion happens only when `VACUUM` runs.

### What VACUUM Does

VACUUM scans the table directory and permanently deletes any Parquet file that:

1. Is not referenced by the current active transaction log (i.e., it has been logically
   removed), **and**
2. Was last referenced more than the configured retention period ago

Default retention: **7 days** (168 hours).

=== "SQL — Dry Run First"

    ```sql
    -- Preview what VACUUM would delete without actually deleting
    VACUUM retaildb.orders RETAIN 168 HOURS DRY RUN;
    ```

=== "SQL — Execute"

    ```sql
    -- Delete files older than 7 days (default)
    VACUUM retaildb.orders;

    -- Explicit 14-day retention
    VACUUM retaildb.orders RETAIN 336 HOURS;
    ```

=== "PySpark"

    ```python
    from delta.tables import DeltaTable

    dt = DeltaTable.forName(spark, "retaildb.orders")

    # Dry run
    dt.vacuum(retentionHours=168)  # returns DataFrame of files to be deleted

    # Permanent deletion
    dt.vacuum()  # uses default 7-day retention
    ```

### Bypassing the 7-Day Safety Check

Delta enforces a minimum retention of 7 days by default. To use a shorter retention (for
testing or special use cases), explicitly disable the check:

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM retaildb.orders RETAIN 1 HOURS;
```

!!! warning "Never VACUUM below your time-travel retention requirement"
    Running VACUUM permanently destroys the old Parquet files that time travel depends on.
    Once those files are gone, you cannot query historical versions that needed them.

    **Rule:** Set VACUUM retention >= the maximum version age you ever need to time-travel
    back to. If your time-travel SLA is 30 days, run `VACUUM RETAIN 720 HOURS`.

    Never run VACUUM with less than 7 days retention in production unless you have
    explicitly verified that no active readers or time-travel queries depend on older
    versions.

---

## Auto-Optimize (Databricks)

Databricks offers two automatic file management features that reduce the need for manual
OPTIMIZE scheduling:

### Optimized Writes

Before writing data to storage, Databricks coalesces small shuffle partitions into larger
ones — targeting files close to the OPTIMIZE target size. This reduces the number of
files created per write, lessening the small-file accumulation rate.

### Auto Compaction

After a write completes, if a partition has accumulated more than a threshold number of
small files, Databricks triggers a lightweight compaction job in the background
automatically.

### Enabling Auto-Optimize

=== "PySpark"

    ```python
    spark.sql("""
        ALTER TABLE retaildb.orders
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    ```

=== "SQL"

    ```sql
    ALTER TABLE retaildb.orders
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    );
    ```

```python
# Or enable globally for the session / cluster
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",  "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",    "true")
```

!!! tip "Auto-optimize does not replace scheduled OPTIMIZE"
    Auto-optimize reduces small-file accumulation but does not perform Z-ORDER. For
    filter-heavy Silver and Gold tables, still schedule a weekly
    `OPTIMIZE ... ZORDER BY (...)` job. Auto-optimize is a complement, not a replacement.

---

## Photon — Vectorized Execution Engine

Photon is Databricks' native query execution engine, implemented in C++. It replaces
Spark's JVM-based row-at-a-time execution with **vectorized column-at-a-time** processing,
achieving 2–10× speedups on CPU-bound analytical workloads.

### What Photon Accelerates

| Workload | Photon Impact |
|---|---|
| Large analytical SQL joins | High — batch processes column batches natively |
| Complex GROUP BY aggregations | High |
| Columnar scan + filter (data skipping) | High |
| Machine learning inference in SQL | Moderate |
| Streaming micro-batches | Moderate |
| UDF-heavy Python workloads | **Low** — Python UDFs bypass the C++ engine |
| Non-Delta formats (ORC, Hive SerDes) | **None** — Photon works on Delta + Parquet only |

### Enabling Photon

Photon is enabled at the cluster level in the Databricks UI:
**Cluster config → Enable Photon Acceleration** (checkbox).

Or via Terraform / DABS:

```yaml
# In a Databricks Asset Bundle cluster spec
spark_version: "14.3.x-photon-scala2.12"
```

Photon is transparent — existing PySpark and SQL code runs unchanged. No code
modifications are required.

!!! info "Photon is not free"
    Photon-enabled clusters have a slightly higher DBU multiplier than standard clusters.
    Benchmark your workloads to confirm the performance gain justifies the cost increase for
    your specific use case. CPU-bound analytical SQL typically sees a clear net saving on
    total wall-clock time × DBU rate. Python-heavy ETL may not.

---

## Liquid Clustering (Databricks)

Liquid Clustering is the modern replacement for static partitioning combined with
Z-ORDER. It addresses the key limitations of both:

| Problem | Static Partitioning + Z-ORDER | Liquid Clustering |
|---|---|---|
| Partition column changes | Requires full table rewrite | Change `CLUSTER BY` with no rewrite |
| Skewed partitions (hot partition) | Manual mitigation required | Handled automatically |
| Multi-column clustering | Z-ORDER is one-shot, not incremental | Background service incrementally clusters |
| New table setup | Choose partition columns upfront, hard to change | Declare `CLUSTER BY`, adjust anytime |

### Creating a Table with Liquid Clustering

```sql
CREATE TABLE retaildb.orders_v2 (
    order_id     INT,
    order_date   DATE,
    customer_id  INT,
    order_status STRING
)
USING DELTA
CLUSTER BY (customer_id, order_date);
```

### Changing the Clustering Column

```sql
-- Change the clustering key without rewriting the table
ALTER TABLE retaildb.orders_v2
CLUSTER BY (customer_id, order_status);
```

### Triggering Clustering Explicitly

```sql
-- Liquid Clustering also uses OPTIMIZE to trigger compaction + clustering
OPTIMIZE retaildb.orders_v2;
```

!!! tip "Use Liquid Clustering for new tables"
    For any new Delta table on Databricks Runtime 13.3+, prefer `CLUSTER BY` over static
    `PARTITIONED BY`. Liquid Clustering is more resilient to changing query patterns and
    eliminates the common mistake of over-partitioning (creating thousands of tiny
    partition directories).

---

## Delta Table Health Checklist

Run this checklist regularly (weekly or after major pipeline runs) on any production Delta
table:

### 1 — Check File Count and Average File Size

```sql
DESCRIBE DETAIL retaildb.orders;
```

Key fields to inspect:

| Field | Healthy Range | Action if Outside Range |
|---|---|---|
| `numFiles` | Hundreds to low thousands | Run OPTIMIZE if tens of thousands |
| `sizeInBytes / numFiles` | 128 MB – 1 GB per file | Run OPTIMIZE to compact |
| `partitionColumns` | Matches query filter patterns | Consider repartitioning or Liquid Clustering |

### 2 — Review Table History

```sql
DESCRIBE HISTORY retaildb.orders;
```

Look for:
- Unusually high `numRemovedFiles` in recent commits (sign of excessive rewrites)
- Long gaps between OPTIMIZE operations
- Any `RESTORE` operations (indicates prior data quality issues)

### 3 — Verify Vacuum Frequency

```sql
-- Check last vacuum timestamp in history
DESCRIBE HISTORY retaildb.orders;
-- Look for operation = 'VACUUM END'
```

A production table should be vacuumed at least weekly. If the last vacuum was more than
30 days ago, unreferenced files are consuming storage and making directory listings slower.

### 4 — Spot-Check Z-ORDER Effectiveness

```python
# Run an EXPLAIN on a typical query and look for "skipped files" in the plan
spark.sql("""
    EXPLAIN FORMATTED
    SELECT * FROM retaildb.orders
    WHERE customer_id = 12345
    AND order_date BETWEEN '2024-01-01' AND '2024-03-31'
""").show(truncate=False)
```

In the output, look for `numFilesSkipped` in the scan metrics. A healthy Z-ORDERed table
skips > 80% of files for selective queries.

### 5 — Auto-Optimize Properties

```sql
SHOW TBLPROPERTIES retaildb.orders;
```

Confirm `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` are set
to `true` for high-frequency write tables.

---

## Optimization Decision Tree

```
Is the table query slow?
│
├── Many small files (numFiles > 5,000 for a < 1 TB table)?
│   └── Run: OPTIMIZE table
│
├── Selective queries filter on specific columns but still scan many files?
│   └── Run: OPTIMIZE table ZORDER BY (filter_col1, filter_col2)
│
├── Storage costs high — old files accumulating?
│   └── Run: VACUUM table RETAIN 168 HOURS
│
├── New table — choose physical layout?
│   ├── Databricks Runtime 13.3+?  → Use CLUSTER BY (Liquid Clustering)
│   └── Older runtime?             → PARTITIONED BY date + ZORDER BY join_key
│
└── Continuous streaming writes producing small files constantly?
    └── Enable: delta.autoOptimize.optimizeWrite + delta.autoOptimize.autoCompact
        AND schedule weekly: OPTIMIZE ZORDER BY (join_key)
```

---

## Quick Reference — Optimization Commands

---

## Interview Q&A

**Q: What is OPTIMIZE and why do you need it?**
OPTIMIZE compacts many small Parquet files into larger target-sized files (default 1 GB).
Small files accumulate from streaming writes, frequent UPDATEs, and DELETEs (each
copy-on-write rewrite can produce a medium-sized file). Many small files increase the
number of Spark tasks, object-storage API calls, and metadata overhead, slowing queries
significantly.

**Q: What is Z-ORDER and how does it help queries?**
Z-ORDER is a multi-dimensional data clustering technique. It reorganises rows so that
records with similar values in the specified columns are stored in the same Parquet files.
This makes the file-level min/max statistics in the Delta transaction log highly selective,
allowing the query engine to skip large numbers of files based on filter predicates — a
technique called data skipping.

**Q: What happens if you run VACUUM with a 1-hour retention?**
Any Parquet file that was logically removed from the Delta table more than 1 hour ago is
permanently deleted from object storage. After this runs, you can no longer time-travel to
versions that depended on those files. This is irreversible — deleted files cannot be
recovered.

**Q: What is the difference between Delta Cache and Spark Cache?**
Delta Cache stores remote ADLS/S3 Parquet files on local worker SSD in an optimised
format, reducing repeated remote storage reads. Spark Cache stores a processed
DataFrame/RDD in JVM heap or off-heap memory to avoid re-computing transformations.
Delta Cache benefits any scan-heavy query; Spark Cache benefits workloads that re-use
the same derived DataFrame many times within a session.

**Q: When should you use Liquid Clustering instead of partitioning?**
Use Liquid Clustering for new tables on Databricks Runtime 13.3+ in almost all cases. It
avoids the common pitfall of over-partitioning (creating millions of tiny directories for
high-cardinality partition keys), handles skewed data distributions automatically, and
allows you to change the clustering key without rewriting the table.

---

## Quick Reference — Optimization Commands

```sql
-- Compact small files (bin packing)
OPTIMIZE retaildb.orders;

-- Compact + Z-ORDER clustering
OPTIMIZE retaildb.orders ZORDER BY (customer_id, order_date);

-- Preview vacuum (dry run — no deletion)
VACUUM retaildb.orders RETAIN 168 HOURS DRY RUN;

-- Execute vacuum (permanent deletion of old files)
VACUUM retaildb.orders RETAIN 168 HOURS;

-- Enable auto-optimize on a table
ALTER TABLE retaildb.orders
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- Liquid Clustering — new table
CREATE TABLE retaildb.orders_v2 (...) USING DELTA CLUSTER BY (customer_id);

-- Liquid Clustering — alter existing table
ALTER TABLE retaildb.orders_v2 CLUSTER BY (customer_id, order_date);

-- Inspect table health
DESCRIBE DETAIL  retaildb.orders;
DESCRIBE HISTORY retaildb.orders;
SHOW TBLPROPERTIES retaildb.orders;
```
