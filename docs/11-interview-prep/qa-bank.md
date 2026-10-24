# Technical Q&A Bank

This bank covers the highest-value technical interview questions for data engineering roles. Questions are grouped by domain. Each answer is written at the depth expected in a senior DE interview — not a one-liner, but concise enough to deliver in 90 seconds.

!!! info "How to Use This Bank"
    Read each Q, cover the answer, and try to recall the key points before reading. Mark the ones you stumble on and revisit them. The goal is fluent recall, not memorisation of exact phrasing.

---

## Apache Spark

**Q: What is the difference between a transformation and an action?**

A transformation is a *lazy* operation that defines a new RDD or DataFrame from an existing one — it records the operation in the DAG but does not execute any computation. Examples: `filter()`, `map()`, `select()`, `join()`. An action is what *triggers* execution — it forces Spark to materialise results by traversing the DAG and running the physical plan. Examples: `count()`, `collect()`, `show()`, `write()`. This distinction matters because Spark can optimise the full execution plan before running any work, and it avoids computing intermediate results that are never consumed.

---

**Q: Explain how Spark handles fault tolerance.**

Spark achieves fault tolerance through **lineage tracking** at the RDD level and **write-ahead logs (WAL)** and **checkpoints** in Structured Streaming. For batch jobs: if a partition is lost because an executor dies, Spark re-computes only that partition by replaying the transformations recorded in the DAG lineage — it does not need to restart the whole job. For streaming: checkpoint files in durable storage (ADLS, S3) record the last successfully committed offset and the streaming query state, allowing exact resumption after failure. Delta Lake adds a further layer — each write is an atomic transaction, so a failed write leaves no partial state.

---

**Q: What causes data skew and how do you resolve it?**

Data skew occurs when the data for a join or aggregation key is distributed unevenly across partitions, causing one or a few tasks to process vastly more data than others — these show up as straggler tasks in the Spark UI with 10–100× longer runtimes.

Common causes:
- Natural key cardinality bias (e.g., a VIP customer with 10M orders while typical customers have 10)
- `null` values all landing in the same partition
- High-cardinality columns with power-law distributions

Resolution strategies:
1. **Broadcast join** — if the smaller side is under the broadcast threshold, broadcast it to eliminate the shuffle entirely.
2. **Salting** — append a random integer suffix to skewed keys on the fact side, explode the dimension side to match, join on salted keys, then drop the salt column.
3. **AQE Skew Join** (`spark.sql.adaptive.skewJoin.enabled=true`) — Spark detects oversized shuffle partitions at runtime and automatically splits them.
4. **Repartition before aggregation** — `df.repartition(200, col("customer_id"))` spreads the data more evenly before a groupBy.

---

**Q: When would you use RDD vs. DataFrame?**

Use a **DataFrame** in almost all production workloads. DataFrames go through the Catalyst optimizer and Tungsten execution engine, which apply predicate pushdown, column pruning, join reordering, and code generation — delivering 10–100× better performance than equivalent RDD code.

Use an **RDD** when:
- You need fine-grained control over the physical representation that the DataFrame API cannot express.
- You are processing unstructured data (binary files, custom serialisation formats) where a schema cannot be applied.
- You are integrating with a legacy library that returns RDDs.
- You need custom partitioning logic via a `Partitioner` (e.g., a custom range partitioner for a specific distribution).

In practice, if you find yourself reaching for RDDs in a new pipeline, first check whether a UDF, pandas UDF, or `map_partitions` on a DataFrame achieves the same goal.

---

**Q: Explain AQE and its three main features.**

**Adaptive Query Execution (AQE)** is a Spark optimisation framework (enabled by default since Spark 3.2) that re-optimises the physical execution plan at runtime using actual shuffle statistics, rather than relying solely on pre-execution estimates from column statistics.

The three main features:

1. **Dynamically coalescing shuffle partitions** — After a shuffle, if the actual partition sizes are much smaller than the target, AQE merges small partitions together to avoid thousands of tiny tasks. Controlled by `spark.sql.adaptive.coalescePartitions.enabled`.

2. **Dynamically switching join strategies** — If the runtime statistics show that one side of a join is smaller than the broadcast threshold (even if the planner didn't know this at compile time), AQE converts a sort-merge join into a broadcast hash join on the fly.

3. **Dynamically handling skew in joins** — If one or more shuffle partitions are significantly larger than the median, AQE splits them into smaller sub-partitions and replicates the matching partition from the other side. Controlled by `spark.sql.adaptive.skewJoin.enabled`.

---

**Q: What is the difference between `repartition()` and `coalesce()`?**

Both change the number of partitions, but they work differently:

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | Can increase or decrease | Can only decrease |
| Shuffle | Full shuffle — data moves across the network | Narrow transformation — moves data on the same node where possible |
| Output balance | Balanced partitions | Potentially unbalanced partitions |
| When to use | Before a join or aggregation where balanced partitions matter | After a filter that dramatically reduces data size, before a write |

A common pattern: after filtering down a 100 GB dataset to 1 GB, call `coalesce(8)` before writing to avoid 200 tiny output files — no shuffle, just combining local partitions.

---

**Q: How does Spark's Catalyst optimizer work?**

Catalyst is a rule-based and cost-based query optimisation framework that transforms a user's logical plan into an optimised physical plan in four stages:

1. **Analysis** — Resolves column names, types, and table references using the catalog. Produces a resolved logical plan.
2. **Logical Optimisation** — Applies rule-based transformations: predicate pushdown (move filters as close to the source as possible), constant folding (evaluate constant expressions at plan time), column pruning (drop columns not referenced downstream).
3. **Physical Planning** — Generates one or more physical plans from the logical plan, choosing join strategies (broadcast, sort-merge, shuffle hash) based on table statistics. Selects the lowest-cost plan.
4. **Code Generation** — Uses Janino to compile the physical plan into JVM bytecode at runtime via the Tungsten engine's whole-stage code generation, eliminating virtual function dispatch overhead.

---

**Q: What is lazy evaluation and why does it matter for performance?**

Lazy evaluation means Spark records transformations as a DAG without executing them immediately. Execution is deferred until an action is called. This matters for performance in three ways:

1. **Full plan optimisation** — Catalyst can see the entire transformation chain before compiling — it can push filters down through joins, prune unused columns early, and reorder operations in ways that would not be possible if each step executed immediately.
2. **No wasted computation** — If you define a DataFrame and then call `df.filter(...).limit(1).collect()`, Spark never fully materialises the filtered DataFrame — it short-circuits as soon as one row is found.
3. **Pipelining** — Multiple narrow transformations (map, filter, select) on the same partition are fused into a single stage pass, reading each record once rather than writing intermediate results to disk.

---

## Delta Lake

**Q: What is the Delta transaction log?**

The Delta transaction log (stored in the `_delta_log/` directory alongside Parquet data files) is a JSON-based ordered log of every operation performed on a Delta table. Each commit is an atomic JSON file containing:

- The operation performed (`WRITE`, `MERGE`, `OPTIMIZE`, `DELETE`)
- The list of files added and removed by that operation
- Schema at commit time
- Statistics (row counts, min/max per column) for each added file

Readers use the transaction log to construct a consistent snapshot of the table at any point in time (time travel), and writers use it to detect concurrent conflicts (optimistic concurrency control). Every 10 commits, Delta generates a Parquet checkpoint file that collapses the log for faster initial reads.

---

**Q: Explain ACID in the context of Delta Lake.**

- **Atomicity** — A write either fully succeeds (all files listed in the commit JSON are present) or fully fails. There are no partial writes visible to readers. If the Spark executor dies mid-write, the uncommitted files are not listed in the transaction log and are invisible to all readers.
- **Consistency** — Schema enforcement ensures that every write conforms to the declared table schema. A write that would add a column or change a type is rejected (unless `mergeSchema` is enabled).
- **Isolation** — Concurrent readers always see a consistent snapshot (the last committed version). Writers use optimistic concurrency control — conflicts are detected by inspecting the transaction log and raises a `ConcurrentModificationException` if two writers modify the same files.
- **Durability** — Once a commit JSON is written to ADLS/S3, the transaction is durable — it persists through node failures, network partitions, and cluster restarts.

---

**Q: What is time travel and when would you use it?**

Time travel lets you query a Delta table as it existed at a previous point in time using a version number or timestamp:

```sql
-- By version number
SELECT * FROM prod.silver.orders VERSION AS OF 42;

-- By timestamp
SELECT * FROM prod.silver.orders TIMESTAMP AS OF '2024-06-01T00:00:00';
```

Use cases:
- **Debugging** — compare the current table state to what it looked like before a bad merge or transformation ran.
- **Audit / compliance** — prove to regulators what data was used to generate a report on a specific date.
- **ML reproducibility** — re-train a model on the exact dataset version used in a previous experiment.
- **Rollback** — restore the table to a previous version after a bad write: `RESTORE TABLE orders TO VERSION AS OF 41`.

Time travel data is available until `VACUUM` deletes files older than the retention threshold (default 7 days).

---

**Q: What is the difference between OPTIMIZE and VACUUM?**

| Command | Purpose | What It Does |
|---|---|---|
| `OPTIMIZE` | Performance | Compacts many small Parquet files into fewer larger files (default target 1 GB). Optionally applies Z-Ordering to co-locate related data. Does not delete anything — old files remain until VACUUM. |
| `VACUUM` | Storage reclamation | Deletes Parquet files that are no longer referenced by the transaction log and are older than the retention threshold (default 168 hours / 7 days). Permanently removes files — cannot be undone. |

!!! warning "VACUUM and Time Travel"
    Running `VACUUM` with a retention period shorter than your time travel window will delete the files needed for those historical queries. Never run `VACUUM RETAIN 0 HOURS` on a production table without understanding this trade-off.

---

**Q: Explain the Medallion architecture and why it is better than a two-layer approach.**

The Medallion architecture defines three distinct zones:

- **Bronze** — Raw, unmodified ingested data. Append-only. Preserves the original source format and full history. Acts as a replay buffer — if downstream processing has a bug, you can re-derive Silver and Gold from Bronze.
- **Silver** — Cleaned, validated, and joined data. Applies data quality rules, deduplication, and schema enforcement. The zone most data engineers and analysts work from for exploratory queries.
- **Gold** — Business-level aggregated and domain-specific views. Optimised for BI tools and reporting — pre-joined, pre-aggregated, often modelled as star schemas.

A two-layer approach (raw + clean) collapses Bronze and Silver, which means:
- Raw data and cleaned data share the same tables — harder to trace data quality issues.
- No clear boundary for when transformations are applied.
- Harder to isolate the impact of schema changes from the upstream source.

The three-layer boundary enforces separation of concerns, enables independent replay of each layer, and gives downstream consumers a stable, high-quality interface (Gold) independent of upstream changes.

---

## Kafka

**Q: What is a consumer group and why does it matter for parallelism?**

A consumer group is a set of consumers that jointly consume a Kafka topic. Kafka guarantees that each partition in the topic is consumed by exactly one consumer within a group at any time. This means:

- If a topic has 10 partitions and a consumer group has 10 consumers, each consumer processes exactly 1 partition in parallel.
- If the group has 5 consumers, 5 consumers each process 2 partitions.
- If the group has 15 consumers, 5 are idle (you cannot have more active consumers than partitions).

Consumer groups enable independent consumption of the same topic. For example, a Spark Structured Streaming job and a separate audit logger can both consume the same topic from different offsets, without interfering with each other.

---

**Q: How does Kafka guarantee message ordering?**

Kafka guarantees ordering **within a single partition**. Messages published to the same partition by the same producer appear in the order they were sent. However, Kafka provides no ordering guarantee *across* partitions.

Design implication: if you need all events for a given entity (e.g., a user, an order) to be processed in order, partition your topic by the entity key (`partitionKey = user_id`). This ensures all events for the same user land in the same partition and are consumed in order.

---

**Q: Explain the trade-off between `acks=1` and `acks=all`.**

The `acks` producer configuration controls how many broker acknowledgements the producer waits for before considering a write successful:

| Setting | Behaviour | Throughput | Durability |
|---|---|---|---|
| `acks=0` | No acknowledgement — fire and forget | Highest | None — messages can be lost |
| `acks=1` | Leader broker acknowledges | High | Medium — message is safe as long as leader survives |
| `acks=all` (or `-1`) | All in-sync replicas acknowledge | Lower | Highest — message survives leader failure |

For financial or audit data, use `acks=all`. For high-volume telemetry where occasional loss is acceptable, `acks=1` reduces producer latency significantly.

---

**Q: What is log compaction and when would you use it?**

Log compaction is a Kafka retention policy (`cleanup.policy=compact`) that retains only the **most recent message for each key** in a partition, rather than messages by time or size. The result is a compacted log that represents the latest state of each key.

Use cases:
- **Change data capture** — retain only the latest state of each database row, not the full change history.
- **Configuration or reference data** — a topic where consumers need to rebuild the current state on startup (e.g., feature flags, user profiles).
- **Event sourcing** — allow new consumers to reconstruct the current application state by reading only the compacted log rather than replaying all events from the beginning.

Log compaction does not delete messages with non-null values immediately — it runs asynchronously and retains the latest version of each key indefinitely.

---

## Azure / Cloud

**Q: What is the difference between ADLS Gen2 and Blob Storage?**

Both are built on the same underlying Azure storage infrastructure, but ADLS Gen2 enables the **Hierarchical Namespace (HNS)**:

| Feature | Azure Blob Storage | ADLS Gen2 |
|---|---|---|
| Namespace | Flat — slashes are part of the blob name | Hierarchical — real physical directory tree |
| Directory operations | Simulated — rename/move requires copying all blobs | Atomic — O(1) rename/move at any directory level |
| Access control | Container and blob-level only | POSIX-style ACLs at file and directory level |
| Best for | General object storage (images, backups, logs) | Analytics workloads (Spark, Databricks, Synapse) |

For any Spark or Databricks workload, always use ADLS Gen2. The atomic rename is critical for Delta Lake's commit protocol.

---

**Q: When would you use Azure Data Factory vs. Databricks for ETL?**

| Use ADF When | Use Databricks When |
|---|---|
| Connecting to 90+ SaaS and on-premises data sources via managed connectors | Performing complex transformations that require Spark's distributed computation |
| Orchestrating dependencies between activities across heterogeneous systems | Processing large data volumes (TB+) where Python/PySpark logic is needed |
| Minimal code preferred — visual pipeline authoring | ML feature engineering or model training alongside ETL |
| Triggering Databricks notebooks as part of a larger orchestration workflow | Custom business logic that is difficult to express in Mapping Data Flows |

In practice, ADF and Databricks are **complementary**: ADF handles orchestration, scheduling, and connectivity to external systems; Databricks handles heavy compute. A common pattern is an ADF pipeline that triggers a Databricks job via the Databricks Notebook activity.

---

**Q: What is Unity Catalog and why is it better than workspace-level Hive metastore?**

Unity Catalog is Databricks' unified governance layer, attached at the account level rather than per workspace. It provides a three-level namespace (`catalog.schema.table`) shared across all workspaces in an account.

Why it is better than workspace Hive metastore:

| Capability | Hive Metastore | Unity Catalog |
|---|---|---|
| Scope | Per workspace — siloed | Account-wide — shared across all workspaces |
| Access control | Workspace-level permissions | Column-level, row-level, fine-grained ANSI SQL ACLs |
| Lineage | None | Automatic end-to-end lineage (table → dashboard) |
| Audit | Limited | Full audit log of every data access |
| Data sharing | Manual export/import | Built-in Delta Sharing protocol |
| External data | Not supported natively | Foreign catalogs (PostgreSQL, MySQL, Snowflake) |

---

**Q: Explain the difference between SAS token, Storage Key, and RBAC in Azure Storage.**

| Mechanism | Access Scope | Duration | Recommended For |
|---|---|---|---|
| **Storage Account Key** | Full account access — all containers, blobs, queues, tables | Permanent until rotated | Administrative operations; store in Key Vault, not in code |
| **SAS Token** | Scoped to specific resource(s), specific permissions (read/write/delete), specific IP ranges | Time-limited (minutes to days) | Granting temporary access to external parties or applications |
| **RBAC (Entra ID)** | Role-based, identity-bound (user, group, service principal, managed identity) | Duration of role assignment | Enterprise applications, Databricks cluster identity, CI/CD pipelines |

Best practice: use RBAC with Managed Identity for all Azure services that support it. Use SAS for third-party or temporary access. Never store Account Keys in application code or pipeline configurations — use Azure Key Vault.

---

## General Data Engineering

**Q: What is idempotency and why is it critical in data pipelines?**

An idempotent operation produces the same result regardless of how many times it is applied. In data pipelines, this means running a job once or running it ten times on the same input produces identical output — no duplicate rows, no missing data.

Why it matters: pipelines fail. Network timeouts, executor crashes, and infrastructure issues cause jobs to be retried. Without idempotency, a retry produces duplicates — inflated revenue numbers, double-counted events, corrupted aggregates.

How to achieve it:
- Use `MERGE INTO` on a natural key rather than `INSERT` — upserts are idempotent; inserts are not.
- Write to a specific partition with `INSERT OVERWRITE PARTITION` rather than appending.
- Assign a deterministic `batch_id` or `event_id` and deduplicate on it in the sink.
- For ADF Copy Activity: use a staging area with `COPY INTO` (idempotent by design) rather than direct append.

---

**Q: Explain SCD Type-2 and how you would implement it in PySpark.**

A Slowly Changing Dimension Type-2 tracks the full history of a dimension by creating a new row for each change, with `effective_start_date`, `effective_end_date`, and `is_current` flag. The previous row's `effective_end_date` is set to yesterday and `is_current` set to `false`.

Implementation in PySpark using Delta Lake MERGE:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws

# Compute hash of all tracked columns to detect changes
incoming = source_df.withColumn(
    "row_hash",
    md5(concat_ws("|", "customer_name", "email", "city"))
)

dim = DeltaTable.forName(spark, "prod.gold.dim_customer")

# Step 1: Expire rows that have changed (matched on biz key, different hash)
dim.alias("t").merge(
    incoming.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true AND t.row_hash != s.row_hash"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_end_date": current_timestamp()
}).execute()

# Step 2: Insert new rows for changed + new records
dim.alias("t").merge(
    incoming.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenNotMatchedInsert(values={
    "customer_id": "s.customer_id",
    "customer_name": "s.customer_name",
    "email": "s.email",
    "city": "s.city",
    "row_hash": "s.row_hash",
    "effective_start_date": current_timestamp(),
    "effective_end_date": lit(None),
    "is_current": lit(True)
}).execute()
```

---

**Q: What is the CAP theorem and how does it apply to distributed systems?**

The CAP theorem states that a distributed data store can only guarantee two of three properties simultaneously:

- **Consistency (C)** — Every read returns the most recent write (or an error). All nodes see the same data at the same time.
- **Availability (A)** — Every request receives a response (not guaranteed to be the most recent data). No failures.
- **Partition Tolerance (P)** — The system continues operating even when network partitions split nodes from each other.

Since network partitions are a physical reality in any distributed system, the practical choice is **CP vs. AP**:

| System | Choice | Implication |
|---|---|---|
| HBase, ZooKeeper | CP | Returns error rather than stale data during partition |
| Cassandra, DynamoDB | AP | Returns potentially stale data to remain available |
| Delta Lake | CP (via optimistic concurrency) | Rejects conflicting writes; readers always see consistent snapshots |

For data engineering: Delta Lake's ACID semantics make it CP — it sacrifices availability (a conflicting write fails) to guarantee consistency. Kafka, by contrast, is AP — a producer can still write during a broker partition, accepting potential replication lag.

---

**Q: How do you handle schema evolution in a data lake?**

Schema evolution refers to the ability to handle upstream schema changes (new columns, renamed columns, type changes) without breaking downstream pipelines.

Strategies:

1. **Schema enforcement (reject)** — Delta Lake's default. Writes that don't match the declared schema are rejected, protecting downstream consumers from surprises.

2. **Schema evolution (accept new columns)** — Enable with `.option("mergeSchema", "true")` on the write. New columns from the source are added to the Delta table schema automatically. Existing rows get `null` for the new column.

3. **Auto Loader with schema inference** — Auto Loader can detect new columns in incoming files and automatically update the Delta table schema. Configure `cloudFiles.schemaEvolutionMode = "addNewColumns"`.

4. **Explicit schema management** — Treat the schema as code: version it in a schema registry (e.g., Confluent Schema Registry for Kafka, or a custom catalog table), validate incoming data against the registered schema, and gate schema changes through a review process.

5. **Schema-on-read with Bronze** — Keep Bronze as raw files (Parquet or JSON) with no enforced schema, apply schema enforcement only in Silver. This decouples ingestion from transformation — schema changes can be absorbed in Bronze without breaking Silver immediately.

!!! tip "Best Practice"
    Enforce schema strictly in Silver and Gold. Allow schema evolution in Bronze with `mergeSchema`. Alert on unexpected new columns rather than silently accepting them — a new column in the source is often a signal of an upstream system change that needs investigation.

---

**Q: How does Spark's memory management work, and what causes OOM errors?**

Spark divides each executor's memory into four regions:

1. **Reserved memory** (~300 MB) — system overhead; not configurable.
2. **User memory** (40% of heap by default) — Python/Java data structures created in UDFs and non-Spark code.
3. **Execution memory** (60% of unified memory pool) — shuffle buffers, sort buffers, join hash tables; can borrow from storage memory if idle.
4. **Storage memory** (60% of unified memory pool, shared with execution) — cached DataFrames and broadcast variables; can be evicted by execution memory under pressure.

Common causes of OOM:
- A single partition is too large to fit in executor memory (data skew or not enough partitions).
- Broadcasting a table that is too large for executor heap — monitor with `spark.sql.autoBroadcastJoinThreshold`.
- A UDF that materialises large intermediate Python objects per row.
- `collect()` returning a result set too large for the driver's heap.

Diagnostic steps: inspect the Spark UI's executor tab for memory usage, check for spill metrics (shuffle spill to disk indicates memory pressure), and increase `spark.executor.memory` or reduce partition size with `repartition()`.

---

**Q: Explain the difference between `reduceByKey` and `groupByKey` in Spark RDDs.**

Both group values by key, but they behave very differently at scale:

`groupByKey()` shuffles **all values** for each key across the network to the reducer before any aggregation happens. If one key has 10 million values, all 10 million values travel across the network and land on one executor — this is the most common cause of OOM errors in RDD code.

`reduceByKey(func)` performs a **partial aggregation (combiner phase) on each partition first**, then shuffles only the partially aggregated result. For a sum of 10 million values across 100 partitions, each partition reduces its 100,000 values to a single number, then only 100 numbers travel across the network.

```python
# BAD: groupByKey shuffles all values — O(N) network traffic per key
result = rdd.groupByKey().mapValues(sum)

# GOOD: reduceByKey partial-aggregates first — O(distinct keys) network traffic
result = rdd.reduceByKey(lambda a, b: a + b)

# For complex aggregations: aggregateByKey
result = rdd.aggregateByKey(
    zeroValue=0,
    seqFunc=lambda acc, val: acc + val,    # within-partition aggregation
    combFunc=lambda acc1, acc2: acc1 + acc2  # cross-partition merge
)
```

In the DataFrame API, `groupBy().agg()` automatically optimises like `reduceByKey` — partial aggregation is applied per partition before the shuffle. This is one of the strongest reasons to prefer DataFrames over RDDs for aggregation workloads.

---

**Q: What is the difference between `cache()` and `persist()` in Spark, and when should you use them?**

Both store a DataFrame or RDD in memory to avoid recomputation on subsequent actions, but `persist()` gives you control over the storage level while `cache()` is a shortcut for `MEMORY_AND_DISK`:

| Storage Level | What It Does |
|---|---|
| `MEMORY_ONLY` | Deserialised objects in JVM heap; fastest access; may OOM on large data |
| `MEMORY_AND_DISK` | Spills to disk if memory is insufficient; `cache()` default |
| `MEMORY_ONLY_SER` | Serialised objects; 5–10× smaller footprint; slower to access |
| `DISK_ONLY` | Never in memory; useful for very large datasets that are read infrequently |
| `OFF_HEAP` | Stores data outside JVM heap using Tungsten unsafe memory; reduces GC pressure |

**When to use:**
- Cache a DataFrame that is used in multiple downstream actions (e.g., used for both a join and a count).
- Do not cache DataFrames that are used only once — the overhead of writing to cache outweighs the benefit.
- Always call `.unpersist()` when the cached data is no longer needed to release memory for other operations.

```python
# Cache the cleaned DataFrame — it's referenced in two separate branches
silver_df = spark.read.format("delta").load("/silver/orders")
silver_df.cache()

# Branch 1: join with customers
joined = silver_df.join(customer_df, "customer_id")

# Branch 2: count for data quality check
record_count = silver_df.count()

# Release when done
silver_df.unpersist()
```

---

## Additional Spark Questions

**Q: Explain the difference between partitioning and bucketing in Spark.**

Both control how data is physically organised on disk, but they serve different purposes:

**Partitioning** creates subdirectories on disk for each distinct value of a column (e.g., `event_date=2024-06-01/`). Queries that filter on the partition column skip directories that don't match — this is **partition pruning**. Partitioning works best on low-cardinality columns (date, region, status) because each unique value creates a directory. High-cardinality columns (user_id, transaction_id) create too many small files.

**Bucketing** distributes rows into a fixed number of files (buckets) based on a hash of a column value. Unlike partitioning, it doesn't create subdirectories — it creates N files per partition. The key benefit is that when two tables are bucketed on the same column with the same bucket count, Spark can perform a sort-merge join without a shuffle — each bucket on one side is joined with the corresponding bucket on the other side. Best for high-cardinality join keys on large tables that are joined repeatedly.

| | Partitioning | Bucketing |
|---|---|---|
| Physical layout | Subdirectories per value | N files per partition |
| Best column type | Low cardinality (date, region) | High cardinality (user_id) |
| Query benefit | Partition pruning — skip directories | Eliminate shuffle on joins/groupBy |
| File count risk | Many small files on high cardinality | Controlled — fixed bucket count |

---

**Q: What is the Tungsten execution engine?**

Tungsten is the Spark execution engine introduced in Spark 1.4 that operates below the DataFrame API to maximize CPU and memory efficiency. It has three main components:

1. **Unsafe Row format** — stores row data off-heap in a compact binary format rather than Java objects. Avoids JVM garbage collection overhead and reduces memory footprint by 5–10×.

2. **Whole-stage code generation** — rather than interpreting the query plan one operator at a time (with virtual function calls), Spark compiles the entire pipeline of operators for a stage into a single JVM bytecode function. This eliminates interpreter overhead and allows the JVM JIT to optimise the tight loop.

3. **Cache-aware data structures** — sorts, aggregations, and hash joins are implemented with CPU cache locality in mind (data access patterns that minimise cache misses).

The practical result: DataFrame/SQL workloads run 5–100× faster than equivalent RDD code for the same logic, and often approach the performance of hand-written C code for common patterns.

---

**Q: What is a broadcast variable and when should you use it?**

A broadcast variable is a read-only variable cached on every executor, sent once rather than serialized with every task. Use it when a large lookup table or configuration dictionary is needed by many tasks:

```python
# Without broadcast: the lookup_dict is serialized once per task
result = df.rdd.map(lambda row: (row.id, lookup_dict[row.country_code]))

# With broadcast: lookup_dict is sent to each executor once, then reused
broadcast_lookup = spark.sparkContext.broadcast(lookup_dict)
result = df.rdd.map(lambda row: (row.id, broadcast_lookup.value[row.country_code]))
```

The DataFrame API's `broadcast()` hint does the same for join optimization — it broadcasts the smaller DataFrame to all executors to eliminate the shuffle. Use broadcast variables explicitly when you have a Python dictionary or list that is too large to re-create per task but too small to model as a DataFrame join.

---

## Additional Delta Lake Questions

**Q: What happens when two writers try to modify the same Delta table simultaneously?**

Delta Lake uses **optimistic concurrency control**. Both writers proceed independently and write their data files. When each writer attempts to commit, it reads the transaction log to check whether any other writer has modified the same files since the writer started.

- If there is **no conflict** (the writers touched different partitions or files), both commits succeed.
- If there is a **conflict** (both writers modified the same files), the second writer raises a `ConcurrentModificationException`. The application must handle this by retrying the transaction.

Conflict detection is based on which files each writer read and wrote. This means append-only operations (e.g., two Spark jobs both appending to different partitions) almost never conflict, while two concurrent MERGE operations on the same rows will conflict.

Delta resolves many common patterns automatically — for example, a reader and writer running simultaneously never conflict because Delta uses snapshot isolation: the reader always sees a consistent snapshot from before the writer's commit.

---

**Q: Explain OPTIMIZE and when you should run it.**

`OPTIMIZE` compacts the many small Parquet files that accumulate from streaming writes, incremental MERGE operations, and frequent small appends into fewer larger files (default target size: 1 GB). This improves query performance because:
- Each file requires a separate read operation — fewer files means fewer open/read calls
- Larger files enable more effective predicate pushdown via row group statistics
- Reduces the number of tasks the query engine must schedule

```sql
-- Basic optimize
OPTIMIZE prod.silver.orders;

-- With Z-Ordering on frequently filtered columns
OPTIMIZE prod.silver.orders ZORDER BY (customer_id, order_date);
```

**When to run it:**
- After each nightly batch load in the Gold layer
- After large MERGE operations that create many small files
- As part of a weekly maintenance job on high-write Silver tables

**Auto-optimize in Databricks:**
- `optimizeWrite` — Databricks automatically coalesces small writes into larger files during the write itself (no post-write compaction needed for that batch)
- `autoCompact` — runs OPTIMIZE automatically after each write if there are too many small files

---

## Additional General Data Engineering Questions

**Q: What is the difference between Lambda Architecture and Kappa Architecture?**

**Lambda Architecture** runs two parallel processing paths for every dataset:
- **Batch layer** — reprocesses the entire historical dataset on a schedule (e.g., nightly) to produce accurate, fully corrected results
- **Speed layer** — processes new data in real-time to produce approximate, low-latency results for the most recent period
- **Serving layer** — merges batch and speed layer outputs at query time

The main criticism of Lambda is that you maintain two separate codebases (batch + streaming) doing the same business logic, which doubles development and maintenance effort and creates subtle inconsistencies when they diverge.

**Kappa Architecture** simplifies this by having a single streaming processing path for both historical and real-time data. Historical reprocessing is handled by replaying the event log (Kafka with long retention) through the same streaming pipeline.

**Lakehouse Architecture** supersedes both for most DE workloads: Spark Structured Streaming with Delta Lake handles both real-time ingestion and batch reprocessing on the same tables, with a single codebase. Delta's ACID transactions ensure consistency between streaming appends and batch corrections.

---

**Q: What is the difference between a star schema and a snowflake schema?**

Both are dimensional modeling patterns for data warehouses:

**Star Schema:** Dimension tables are denormalized — all attributes of a dimension (e.g., product name, category, subcategory, brand) are stored in a single wide table. The result looks like a star: a central fact table surrounded by flat dimension tables. Queries are simple (one join per dimension) and fast. The standard choice for most BI workloads.

**Snowflake Schema:** Dimension tables are normalized — hierarchical attributes are broken into separate tables (e.g., a `product` dimension splits into `product`, `category`, and `brand` tables). Reduces data redundancy but increases join complexity. Queries require more joins, which is slower for interactive BI.

| | Star Schema | Snowflake Schema |
|---|---|---|
| Dimension tables | Denormalized (wide) | Normalized (multiple tables) |
| Storage | More redundancy | Less redundancy |
| Query complexity | Simple — one join per dimension | Complex — multiple joins per hierarchy |
| Query performance | Faster for BI | Slower for BI |
| Maintenance | Easier | More rigid — hierarchy changes require schema changes |

In practice, star schema is almost always preferred for analytics. Snowflake is used when storage cost is the dominant constraint or when the hierarchy is very deep and changes frequently.

---

**Q: How do you monitor and alert on data quality in a production pipeline?**

Data quality monitoring operates at three levels:

**1. Schema validation** — Enforce at write time using Delta's schema enforcement. Add an explicit schema check at ingestion: compare incoming column names and types against the expected schema and fail-fast with a clear error message.

**2. Statistical / row-level checks** — After each pipeline run, validate:
- Row count is within expected range (e.g., daily orders never drop below 1,000 or exceed 10M without an alert)
- Null rate on non-nullable fields is zero
- Key metrics (total revenue, event count) are within X% of the previous day (z-score anomaly detection)
- Referential integrity: all `customer_id` values in the fact table exist in the dimension table

Tools: **Great Expectations**, **AWS Deequ**, **Databricks Lakehouse Monitoring**, or custom SQL assertions in the pipeline.

**3. Freshness monitoring** — Check that the most recent record in the table is not older than expected. A pipeline that runs silently but writes no rows is a common failure mode that schema validation won't catch.

```sql
-- Example: alert if the latest record in orders is more than 2 hours old
SELECT MAX(ingested_at) AS latest_ingest,
       CURRENT_TIMESTAMP() - MAX(ingested_at) AS lag_hours
FROM prod.silver.orders
HAVING lag_hours > 2;
```

**Alerting:** Route pipeline failures and quality check failures to a dedicated Slack channel or PagerDuty rotation. Every alert must have a clear owner, a runbook, and an SLA for response time.
