# Change Data Feed (CDF)

Change Data Feed (CDF) is Delta Lake's built-in mechanism for capturing row-level changes
— INSERT, UPDATE, and DELETE — as they are committed to a table. Instead of comparing
full table snapshots or running expensive hash-based diff queries, downstream pipelines
can read only the rows that actually changed and process them incrementally.

CDF is the standard way to propagate data from Bronze to Silver and from Silver to Gold in
a Medallion pipeline without full table scans.

---

## What CDF Captures

For every DML operation committed to a CDF-enabled Delta table, Delta records the
affected rows along with metadata describing the change type:

| DML Operation | Rows Written to CDF | `_change_type` Values |
|---|---|---|
| `INSERT` | Every inserted row | `insert` |
| `UPDATE` | Each updated row appears **twice** | `update_preimage` (old value) + `update_postimage` (new value) |
| `DELETE` | Every deleted row | `delete` |
| `MERGE` | Each matched/not-matched row | `insert`, `update_preimage`, `update_postimage`, or `delete` depending on clause |

The double row for UPDATE is intentional — it lets consumers compute the exact delta
between the old and new value of any column.

### CDF Metadata Columns

Every row returned from a CDF read includes these system columns:

| Column | Type | Meaning |
|---|---|---|
| `_change_type` | STRING | One of: `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | LONG | The Delta table version in which this change was committed |
| `_commit_timestamp` | TIMESTAMP | Wall-clock time of the commit |

### Example — What a CDF Read Returns

Suppose the following operations were committed to `retaildb.orders`:

- Version 1: INSERT of order_id = 101
- Version 2: UPDATE order_id = 101 → change order_status from PENDING to PROCESSING
- Version 3: DELETE order_id = 101

```
order_id  order_status  _change_type       _commit_version  _commit_timestamp
101       PENDING       insert             1                2024-01-01 09:00:00
101       PENDING       update_preimage    2                2024-01-01 10:00:00
101       PROCESSING    update_postimage   2                2024-01-01 10:00:00
101       PROCESSING    delete             3                2024-01-01 11:00:00
```

---

## Enabling CDF

### On Table Creation

=== "PySpark"

    ```python
    spark.sql("""
        CREATE TABLE retaildb.orders (
            order_id     INT,
            order_date   STRING,
            customer_id  INT,
            order_status STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    ```

=== "SQL"

    ```sql
    CREATE TABLE retaildb.orders (
        order_id     INT,
        order_date   STRING,
        customer_id  INT,
        order_status STRING
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true);
    ```

### On an Existing Table

=== "PySpark"

    ```python
    spark.sql("""
        ALTER TABLE retaildb.orders
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    ```

=== "SQL"

    ```sql
    ALTER TABLE retaildb.orders
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
    ```

!!! note "CDF only captures changes from the point it is enabled"
    Enabling CDF on an existing table does not backfill change records for historical
    versions. CDF records are available starting from the commit version immediately after
    enablement. Your first `startingVersion` should be set to the version at or after
    enablement.

### As a Session Default

```sql
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```

This sets CDF on all new Delta tables created in the session. Useful for development
environments; not recommended for production (prefer explicit table-level control).

---

## Reading CDF — Batch

### By Version

=== "PySpark"

    ```python
    # Read all changes from version 1 onwards (through the latest available version)
    changes_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .table("retaildb.orders")
    )

    # Read changes between two specific versions (inclusive)
    changes_v1_to_v3 = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .option("endingVersion", 3)
        .table("retaildb.orders")
    )
    ```

=== "SQL"

    ```sql
    -- Changes from version 1 to latest
    SELECT * FROM table_changes('retaildb.orders', 1);

    -- Changes between version 1 and version 3 (inclusive)
    SELECT * FROM table_changes('retaildb.orders', 1, 3);
    ```

### By Timestamp

=== "PySpark"

    ```python
    changes_df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingTimestamp", "2024-01-01T00:00:00")
        .option("endingTimestamp",   "2024-01-02T00:00:00")
        .table("retaildb.orders")
    )
    ```

=== "SQL"

    ```sql
    SELECT *
    FROM table_changes('retaildb.orders',
        '2024-01-01T00:00:00',
        '2024-01-02T00:00:00');
    ```

---

## Reading CDF — Structured Streaming

CDF can be consumed as a streaming source, which is ideal for Silver → Gold pipelines
that need near-real-time propagation:

```python
# Streaming CDF source — reads new changes as they arrive
cdf_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("retaildb.orders_silver")
)

# Write change stream to a downstream table
(
    cdf_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation",
            "abfss://checkpoints@storage.dfs.core.windows.net/cdf/orders_gold/")
    .trigger(availableNow=True)
    .toTable("retaildb.orders_gold_changes")
)
```

!!! tip "Use trigger(availableNow=True) for scheduled CDF streaming"
    For scheduled jobs (hourly, daily), `trigger(availableNow=True)` processes all pending
    CDF changes since the last checkpoint, then stops. This gives you incremental streaming
    semantics in a batch job context — lower cost than continuous streaming, more current
    than a full Gold rebuild.

---

## Primary Use Case: Silver → Gold Incremental Propagation

The canonical CDF pattern in a Medallion pipeline:

```
Bronze CDF changes
      |
      v
Filter valid / relevant change types
      |
      v
table_changes() or readChangeFeed
      |
      v
MERGE INTO Silver  (upsert — handles inserts, updates, deletes)
      |
      v
Silver CDF changes
      |
      v
MERGE INTO Gold  (aggregate incrementally)
```

### Full CDF + MERGE Pattern

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date, current_timestamp

# --- Bronze → Silver ---

# 1. Read only meaningful change types from Bronze CDF
#    (skip update_preimage — we only need the final state)
bronze_changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed_bronze_version + 1)
    .table("retaildb.orders_bronze")
    .filter(col("_change_type").isin("insert", "update_postimage"))
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id") > 0)
    .withColumn("order_date", to_date("order_date", "yyyy-MM-dd"))
    .dropDuplicates(["order_id"])
)

# 2. Apply changes to Silver via MERGE
silver = DeltaTable.forName(spark, "retaildb.orders_silver")

(
    silver.alias("tgt")
    .merge(bronze_changes.alias("src"), "tgt.order_id = src.order_id")
    .whenMatchedUpdate(set={
        "order_status": "src.order_status",
        "modifiedon":   "current_timestamp()"
    })
    .whenNotMatchedInsert(values={
        "order_id":     "src.order_id",
        "order_date":   "src.order_date",
        "customer_id":  "src.customer_id",
        "order_status": "src.order_status",
        "createdon":    "current_timestamp()",
        "modifiedon":   "current_timestamp()"
    })
    .execute()
)

# 3. Record the highest Bronze version processed (store in a watermark table)
new_high_watermark = (
    bronze_changes
    .agg({"_commit_version": "max"})
    .collect()[0][0]
)
```

!!! info "Tracking the CDF watermark"
    Store `last_processed_bronze_version` in a small Delta watermark table or a Databricks
    secret. Increment it after each successful run. This gives you exactly-once incremental
    processing — no rows are skipped, no rows are double-processed.

---

## CDF vs Full Table Scan — Performance Comparison

| Scenario | Full Scan | CDF |
|---|---|---|
| Silver table: 500 M rows, 100 GB | Reads 100 GB every run | Reads only changed rows — often < 1 MB |
| Gold refresh after 10,000 new orders | Re-aggregates all 500 M rows | Aggregates delta of 10,000 rows |
| Cost per Silver → Gold refresh | High (full cluster, long runtime) | Minimal (small cluster, seconds) |
| Handles late-arriving corrections | Re-reads everything | `update_preimage` / `update_postimage` shows exact correction |

For tables with low change rates relative to their total size, CDF provides
orders-of-magnitude improvement in pipeline cost and latency.

---

## Where CDF Data is Stored

For non-insert DML operations (UPDATE, DELETE, MERGE), Delta writes the changed rows to
a `_change_data/` directory inside the table path, alongside the regular Parquet files:

```
orders/
├── _delta_log/
│   └── ...
├── _change_data/
│   ├── cdc-00001-...snappy.parquet   ← UPDATE / DELETE row images
│   └── cdc-00002-...snappy.parquet
├── part-00000.snappy.parquet
└── part-00001.snappy.parquet
```

INSERT changes are derived directly from the regular `add` actions in the transaction log
— no separate CDF file is written for inserts.

!!! warning "VACUUM removes CDF data"
    The `_change_data/` files are subject to `VACUUM` like any other file. If you VACUUM
    with a 1-day retention, CDF reads going back more than 1 day will fail. Set VACUUM
    retention to match the maximum lookback window your CDF consumers might need.

---

## Limitations

- **Delta tables only** — CDF is a Delta Lake feature; plain Parquet or CSV tables do not
  support it
- **Must be enabled before changes are made** — there is no retroactive backfill of CDF
  records for changes that occurred before enablement
- **Consumer must track version** — CDF does not maintain a per-consumer offset; it is the
  consumer's responsibility to record which version was last processed and pass
  `startingVersion` accordingly
- **Storage overhead** — CDF files add a modest storage cost for high-volume UPDATE/DELETE
  tables; for insert-only Bronze tables the overhead is zero
- **Not supported on streaming tables in DLT** — Delta Live Tables manages its own change
  propagation; use `APPLY CHANGES INTO` instead of CDF for DLT-managed tables

---

## Watermark Tracking Pattern

A production CDF pipeline must durably record which version of the source table was last
successfully processed. Without this, a pipeline restart would re-process all changes from
version 1, causing duplicate writes downstream.

The recommended pattern is to store the high-watermark version in a small Delta
watermark table:

```python
# --- Watermark Table Setup (run once) ---
spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline.watermarks (
        table_name  STRING,
        last_version LONG,
        updated_at   TIMESTAMP
    ) USING DELTA
""")

# Insert initial watermark
spark.sql("""
    INSERT INTO pipeline.watermarks VALUES (
        'retaildb.orders_bronze', 0, CURRENT_TIMESTAMP()
    )
""")
```

```python
from pyspark.sql.functions import col, current_timestamp, max as spark_max

def run_bronze_to_silver():
    # 1. Read current watermark
    wm = spark.table("pipeline.watermarks") \
              .filter("table_name = 'retaildb.orders_bronze'") \
              .select("last_version") \
              .collect()[0]["last_version"]

    # 2. Read CDF from (watermark + 1) to latest
    changes = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", wm + 1)
        .table("retaildb.orders_bronze")
        .filter(col("_change_type").isin("insert", "update_postimage"))
    )

    if changes.count() == 0:
        print("No new changes — skipping Silver MERGE")
        return

    # 3. Apply Silver MERGE (see delta-core.md for full MERGE code)
    apply_silver_merge(changes)

    # 4. Update watermark to the highest version we just processed
    new_wm = changes.agg(spark_max("_commit_version")).collect()[0][0]
    spark.sql(f"""
        UPDATE pipeline.watermarks
        SET last_version = {new_wm}, updated_at = CURRENT_TIMESTAMP()
        WHERE table_name = 'retaildb.orders_bronze'
    """)
    print(f"Watermark advanced to version {new_wm}")
```

---

## CDF with Deletes — Propagating Hard Deletes to Silver

By default, a MERGE that only handles `insert` and `update_postimage` change types will
silently ignore deleted rows in Bronze. To propagate hard deletes to Silver:

```python
from delta.tables import DeltaTable

# Read all three meaningful change types
bronze_changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", wm + 1)
    .table("retaildb.orders_bronze")
    .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
)

# Separate inserts/updates from deletes
upserts = bronze_changes.filter(
    col("_change_type").isin("insert", "update_postimage")
).dropDuplicates(["order_id"])

deletes = bronze_changes.filter(
    col("_change_type") == "delete"
).select("order_id").dropDuplicates()

silver = DeltaTable.forName(spark, "retaildb.orders_silver")

# Apply upserts
if upserts.count() > 0:
    (silver.alias("tgt")
     .merge(upserts.alias("src"), "tgt.order_id = src.order_id")
     .whenMatchedUpdate(set={"order_status": "src.order_status",
                              "modifiedon": "current_timestamp()"})
     .whenNotMatchedInsertAll()
     .execute())

# Apply deletes separately
if deletes.count() > 0:
    silver.delete(
        f"order_id IN ({','.join(str(r.order_id) for r in deletes.collect())})"
    )
```

!!! tip "Soft deletes are usually preferable"
    Rather than physically deleting rows from Silver, consider adding an `is_deleted BOOLEAN`
    column and setting it to `true` via MERGE WHEN NOT MATCHED BY SOURCE. This preserves
    full history in Silver and makes it trivial to restore accidentally deleted records by
    re-running the pipeline from a Bronze time-travel version.

---

## CDF Quick Reference

```python
# Enable CDF
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

# Batch read by version
df = (spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 5)
      .table("my_table"))

# Batch read by timestamp
df = (spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingTimestamp", "2024-06-01")
      .table("my_table"))

# Batch read — version range
df = (spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 5)
      .option("endingVersion", 10)
      .table("my_table"))

# Streaming read (continuous)
stream = (spark.readStream.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", 5)
          .table("my_table"))

# Filter to only post-update rows (skip preimage noise)
df.filter(col("_change_type").isin("insert", "update_postimage", "delete"))

# SQL: table_changes()
spark.sql("SELECT * FROM table_changes('my_table', 5)")
spark.sql("SELECT * FROM table_changes('my_table', 5, 10)")
spark.sql("SELECT * FROM table_changes('my_table', '2024-01-01', '2024-02-01')")
```

| Option | Values | Meaning |
|---|---|---|
| `readChangeFeed` | `"true"` | Enable CDF mode on this read |
| `startingVersion` | Integer | Read from this Delta version onwards |
| `endingVersion` | Integer | Read up to and including this version |
| `startingTimestamp` | ISO-8601 string | Read from the first version at or after this time |
| `endingTimestamp` | ISO-8601 string | Read up to the last version before or at this time |
