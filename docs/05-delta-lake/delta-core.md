# Delta Lake Core Concepts

This page covers the internals of the Delta transaction log, ACID isolation guarantees,
every DML operation with worked examples, time travel, schema management, idempotent
ingestion with COPY INTO, and data constraints.

---

## The Transaction Log in Depth

### Directory Structure

The `_delta_log/` directory lives at the root of every Delta table path. Its contents follow
a strict naming convention:

```
_delta_log/
├── 00000000000000000000.json    ← version 0 (table creation)
├── 00000000000000000001.json    ← version 1
├── 00000000000000000002.json    ← version 2
├── ...
├── 00000000000000000009.json    ← version 9
├── 00000000000000000010.checkpoint.parquet  ← checkpoint at version 10
├── 00000000000000000011.json    ← version 11
└── _last_checkpoint             ← JSON pointer: {"version": 10, "size": 1}
```

**JSON commit files** — one per transaction, numbered sequentially starting at
`00000000000000000000.json`. Each file is small (typically a few KB to a few hundred KB)
and contains a list of actions that describe what changed in that version.

**Checkpoints** — every 10 commits, Delta writes a `N.checkpoint.parquet` that encodes
the full table state as of version N (all active `add` entries, current schema, partition
columns). Checkpoints eliminate the need to replay all JSON files from version 0.

**`_last_checkpoint`** — a tiny JSON file that records which checkpoint is the most recent.
Readers open this first to find the latest checkpoint, then replay only the JSON commits
that follow it.

### Reading a Delta Table Step by Step

```
1. Read _last_checkpoint  →  learn that latest checkpoint is at version 100
2. Load 100.checkpoint.parquet  →  get full snapshot at v100
3. Replay 00000000000000000101.json … through latest  →  apply subsequent changes
4. Build active file list = (adds from log) - (removes from log)
5. Apply data skipping using min/max stats stored in add actions
6. Read only qualifying Parquet files
```

!!! info "Why checkpoints matter"
    Without checkpoints, reading a table that had 100,000 commits would require replaying
    100,000 JSON files. Checkpoints cap this at at most 9 JSON files beyond the most recent
    checkpoint, keeping reads fast even on long-lived high-write tables.

### What a Commit JSON Looks Like

A typical UPDATE commit contains two actions — a `remove` for the old file and an `add`
for the new file — plus a `commitInfo` entry:

```json
{"commitInfo": {"timestamp": 1690000000000, "operation": "UPDATE",
  "operationParameters": {"predicate": "order_status = 'PENDING'"}}}
{"remove": {"path": "part-00001-abc.snappy.parquet", "deletionTimestamp": 1690000000000}}
{"add": {"path": "part-00007-xyz.snappy.parquet", "size": 1048576,
  "stats": "{\"numRecords\":5000,\"minValues\":{\"order_id\":1},\"maxValues\":{\"order_id\":9999}}"}}
```

The `stats` field inside `add` is what Delta uses for **data skipping** — no file read
required.

---

## ACID in Delta Lake

### Serializable Isolation (Writes)

Delta Lake uses **optimistic concurrency control** for writes. The protocol is:

1. Writer reads the current log version (say, version 12).
2. Writer performs its computation and prepares new Parquet files.
3. Writer attempts to commit version 13 by atomically writing
   `00000000000000000013.json`.
4. If another writer already committed version 13 first, Delta checks whether the two
   operations **conflict**:
   - If they touched disjoint data (e.g., different partitions), the late writer retries
     as version 14.
   - If they are irreconcilable (e.g., both updated the same rows), the late writer
     raises a `ConcurrentModificationException`.

This gives **serializable** isolation for writes — every successful transaction appears to
execute atomically in a well-defined order.

### Snapshot Isolation (Reads)

Readers always operate against a **fixed snapshot** — the set of files that were active at
the moment the read began. A concurrent write that commits while a long-running query is
executing does not affect that query. The reader simply continues using the file list it
resolved at start time.

```
Timeline:
  t=0   Reader opens snapshot at version 5  →  reads files {A, B, C}
  t=1   Writer commits version 6  →  adds file D, removes file A
  t=2   Reader returns rows  →  still reading from {A, B, C}  ← consistent
```

This is the same guarantee that analytical databases like PostgreSQL provide with
repeatable-read transactions.

---

## DML Operations

### INSERT INTO / Append

Append writes new Parquet files and records `add` actions in the log. Existing files are
never touched.

=== "PySpark"

    ```python
    # Append a DataFrame to an existing Delta table
    new_orders_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("retaildb.orders")
    ```

=== "SQL"

    ```sql
    INSERT INTO retaildb.orders
    SELECT order_id, order_date, customer_id, order_status
    FROM staging.new_orders;
    ```

After the commit: the log records `add` actions for the new files. All existing files
remain active. The table version increments by 1.

---

### UPDATE — Copy-on-Write

Delta Lake cannot modify a row in place inside an immutable Parquet file. Instead, it
rewrites every Parquet file that contains at least one matching row:

```
Before UPDATE:
  part-00001.parquet  [rows: 1-5000, some with order_status = 'PENDING']
  part-00002.parquet  [rows: 5001-10000, none match]

Operation:
  UPDATE orders SET order_status = 'PROCESSING' WHERE order_status = 'PENDING'

After:
  part-00001.parquet  → REMOVED from snapshot (log records REMOVE)
  part-00005.parquet  → new file with the same rows but updated status (log records ADD)
  part-00002.parquet  → unchanged (no matching rows, not rewritten)
```

=== "PySpark"

    ```python
    from delta.tables import DeltaTable

    dt = DeltaTable.forName(spark, "retaildb.orders")
    dt.update(
        condition="order_status = 'PENDING'",
        set={"order_status": "'PROCESSING'"}
    )
    ```

=== "SQL"

    ```sql
    UPDATE retaildb.orders
    SET order_status = 'PROCESSING'
    WHERE order_status = 'PENDING';
    ```

!!! note "Copy-on-write trade-off"
    Rewriting files is more expensive than an in-place row edit, but it keeps Parquet files
    immutable (essential for snapshot isolation and time travel). For very high UPDATE
    frequency, consider **Deletion Vectors** (Databricks Runtime 12.2+) which mark deleted
    rows in a small sidecar file instead of rewriting the whole Parquet file.

---

### DELETE

Delete follows the same copy-on-write pattern as UPDATE:

=== "PySpark"

    ```python
    dt = DeltaTable.forName(spark, "retaildb.orders")
    dt.delete("order_status = 'CANCELLED' AND order_date < '2022-01-01'")
    ```

=== "SQL"

    ```sql
    DELETE FROM retaildb.orders
    WHERE order_status = 'CANCELLED'
      AND order_date < '2022-01-01';
    ```

The transaction log records `remove` for the old files and `add` for the rewritten files
that exclude the deleted rows. The physical files are retained on disk until `VACUUM` runs
(default 7-day retention).

---

### MERGE — Upsert

MERGE is the most powerful Delta operation. It combines an INSERT, UPDATE, and
optionally DELETE into a single atomic transaction. It is the workhorse of Silver layer
pipelines.

Full syntax with all three clauses:

=== "PySpark"

    ```python
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, "retaildb.orders_silver")

    (
        target.alias("tgt")
        .merge(
            source_df.alias("src"),
            "tgt.order_id = src.order_id"
        )
        # Row exists in both target and source → update it
        .whenMatchedUpdate(set={
            "order_status": "src.order_status",
            "customer_id":  "src.customer_id",
            "modifiedon":   "current_timestamp()"
        })
        # Row exists in source but not in target → insert it
        .whenNotMatchedInsert(values={
            "order_id":     "src.order_id",
            "order_date":   "to_date(src.order_date)",
            "customer_id":  "src.customer_id",
            "order_status": "src.order_status",
            "createdon":    "current_timestamp()",
            "modifiedon":   "current_timestamp()"
        })
        # Row exists in target but not in source → delete it (soft deletes)
        .whenNotMatchedBySourceDelete()
        .execute()
    )
    ```

=== "SQL"

    ```sql
    MERGE INTO retaildb.orders_silver AS tgt
    USING orders_bronze_changes      AS src
       ON tgt.order_id = src.order_id

    -- Row exists in both: update fields that may have changed
    WHEN MATCHED THEN
      UPDATE SET
        tgt.order_status = src.order_status,
        tgt.customer_id  = src.customer_id,
        tgt.modifiedon   = CURRENT_TIMESTAMP()

    -- Row is new in source: insert into silver
    WHEN NOT MATCHED THEN
      INSERT (order_id, order_date, customer_id, order_status, createdon, modifiedon)
      VALUES (
        src.order_id,
        TO_DATE(src.order_date),
        src.customer_id,
        src.order_status,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      )

    -- Row disappeared from source: mark as deleted in target
    WHEN NOT MATCHED BY SOURCE THEN
      UPDATE SET tgt.order_status = 'DELETED';
    ```

!!! tip "MERGE performance tip"
    MERGE performs a full join between source and target by default. On large tables, always
    run `OPTIMIZE ... ZORDER BY (join_key)` on the target table beforehand. This colocalises
    matching rows in fewer files and reduces the amount of data scanned during the merge.

---

## Time Travel

Delta's transaction log retains the history of every version, making historical reads
trivial. Three mechanisms are available:

### VERSION AS OF

=== "PySpark"

    ```python
    # Read the table as it looked at version 3
    df_v3 = (
        spark.read.format("delta")
        .option("versionAsOf", 3)
        .table("retaildb.orders")
    )
    ```

=== "SQL"

    ```sql
    SELECT *
    FROM retaildb.orders
    VERSION AS OF 3;
    ```

### TIMESTAMP AS OF

=== "PySpark"

    ```python
    df_ts = (
        spark.read.format("delta")
        .option("timestampAsOf", "2024-01-01T00:00:00")
        .table("retaildb.orders")
    )
    ```

=== "SQL"

    ```sql
    SELECT *
    FROM retaildb.orders
    TIMESTAMP AS OF '2024-01-01T00:00:00';
    ```

### RESTORE TABLE

If a bad write corrupted the table, roll it back to any earlier version:

```sql
-- Roll the table back to version 0 (the initial state)
RESTORE TABLE retaildb.orders TO VERSION AS OF 0;

-- Roll back to a specific timestamp
RESTORE TABLE retaildb.orders TO TIMESTAMP AS OF '2024-06-01T08:00:00';
```

### Inspecting History

```sql
DESCRIBE HISTORY retaildb.orders;
```

This returns one row per version: timestamp, user, operation name, operation parameters,
cluster ID, and read/write metrics.

!!! warning "Time travel requires old files"
    Time travel only works as long as the underlying Parquet files for that version still
    exist on disk. Running `VACUUM` with an aggressive retention (less than 7 days) deletes
    old files and destroys the ability to time travel back past that point. Always align your
    VACUUM retention with your time travel requirements.

---

## Schema Enforcement

Delta Lake rejects writes that do not match the table's declared schema. This happens at
commit time — before any files are written.

**What triggers a schema enforcement error:**

- A new column present in the source DataFrame but absent from the table schema
- A column with a different (incompatible) data type than declared
- A nullable column receiving a non-null constraint violation

```python
# This will raise AnalysisException if new_df has extra columns or type mismatches
new_df.write.format("delta").mode("append").saveAsTable("retaildb.orders")
```

Error message example:

```
AnalysisException: A schema mismatch detected when writing to the Delta table.
To enable schema migration, set:
  '.option("mergeSchema", "true")'
```

Schema enforcement protects production tables from accidental data corruption caused by
upstream schema changes.

---

## Schema Evolution

When a source adds new columns that should be accepted into the table, use
`mergeSchema`. Delta will add the new columns to the table schema and backfill `null`
for all existing rows in those columns.

=== "PySpark"

    ```python
    # Allow new columns to be added automatically
    df_with_new_col.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("retaildb.orders")
    ```

    ```python
    # Enable globally for the session
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    ```

=== "SQL"

    ```sql
    -- Explicitly add a column via DDL
    ALTER TABLE retaildb.orders
    ADD COLUMNS (delivery_address STRING AFTER customer_id);
    ```

!!! note "Schema enforcement vs schema evolution"
    **Schema enforcement** (default) — reject writes that don't match. Protects production tables.
    **Schema evolution** (`mergeSchema = true`) — accept writes with new columns. Use during
    intentional schema migrations. Never enable `autoMerge` globally on production clusters.

---

## COPY INTO — Idempotent File Ingestion

`COPY INTO` loads files from an external location into a Delta table. Its key property is
**idempotency**: it tracks which files have already been loaded and skips them on
subsequent runs. This makes it safe to run repeatedly against a landing zone.

```sql
COPY INTO retaildb.orders_bronze
FROM (
    SELECT
        order_id::INT,
        order_date::STRING,
        customer_id::INT,
        order_status::STRING,
        INPUT_FILE_NAME()   AS filename,
        CURRENT_TIMESTAMP() AS createdon
    FROM 'dbfs:/FileStore/raw/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header'      = 'true',
    'inferSchema' = 'true'
);
```

### COPY INTO vs Auto Loader

| | COPY INTO | Auto Loader |
|---|---|---|
| **Idempotent** | Yes | Yes |
| **Scale** | Thousands of files | Millions of files (event-driven) |
| **Schema inference** | Per run | Persistent schema location |
| **Streaming support** | No | Yes (`readStream`) |
| **Use case** | Batch micro-loads, ad hoc re-ingestion | Continuous incremental ingestion |

!!! tip "When to use COPY INTO"
    Use COPY INTO for small-to-medium batch landing zones where you need simplicity and
    idempotency without setting up a streaming checkpoint. Use Auto Loader when file volumes
    are large or when you need continuous near-real-time ingestion.

---

## Data Constraints

Delta tables support declarative data quality constraints enforced at write time. A write
that violates a constraint is rejected with an exception — no partial data is committed.

### NOT NULL Constraint

```sql
ALTER TABLE retaildb.orders
ALTER COLUMN customer_id SET NOT NULL;
```

Attempting to insert a row with `NULL` in `customer_id` will now fail:

```
DeltaInvariantViolationException: NOT NULL constraint violated for column: customer_id.
```

### CHECK Constraint

CHECK constraints enforce arbitrary boolean expressions:

```sql
ALTER TABLE retaildb.orders
ADD CONSTRAINT status_check
CHECK (
    order_status IN (
        'CLOSED', 'COMPLETE', 'PROCESSING',
        'PENDING', 'ON_HOLD', 'CANCELED'
    )
);
```

```sql
-- View all constraints on a table
SHOW TBLPROPERTIES retaildb.orders;

-- Remove a constraint
ALTER TABLE retaildb.orders
DROP CONSTRAINT status_check;
```

!!! warning "Constraints apply to future writes only"
    Adding a CHECK or NOT NULL constraint does **not** retroactively validate existing data.
    Delta will not scan existing rows — only new writes are checked. If you need to validate
    existing data, run a separate quality check query before adding the constraint, or use
    Delta Live Tables expectations which enforce quality at pipeline definition time.

---

## Summary — Core Operations Reference

| Operation | PySpark API | SQL | What it does to the log |
|---|---|---|---|
| Append | `.mode("append")` | `INSERT INTO` | Adds `add` actions for new files |
| Overwrite | `.mode("overwrite")` | `INSERT OVERWRITE` | Removes all existing files, adds new |
| Update | `DeltaTable.update()` | `UPDATE` | `remove` old file, `add` rewritten file |
| Delete | `DeltaTable.delete()` | `DELETE` | `remove` old file, `add` rewritten file |
| Merge | `DeltaTable.merge()` | `MERGE INTO` | Combination of remove + add actions |
| Time travel (read) | `.option("versionAsOf", N)` | `VERSION AS OF N` | Read-only, no log write |
| Restore | `DeltaTable.restoreToVersion()` | `RESTORE TABLE … TO VERSION AS OF N` | Adds a new commit that reinstates old file set |
| Schema evolve | `.option("mergeSchema","true")` | `ALTER TABLE ADD COLUMNS` | Updates `metaData` action in log |
| Add constraint | — | `ALTER TABLE ADD CONSTRAINT` | Updates `metaData` action in log |
