# Resource Sizing & Estimation

Accurately sizing a Spark cluster for production is part science and part trade-off. Allocating the maximum available resources does not guarantee proportional performance improvement — it may waste budget while leaving bottlenecks in place. The goal is to match resources to the data volume, the processing shape (aggregation vs. join vs. sort), and the SLA requirement.

!!! info "The core insight"
    Disk data is compressed and serialized. When loaded into Spark's execution memory, the in-memory representation expands by a factor of **4–6×**. A 100 GB Parquet dataset can require 400–600 GB of aggregate executor memory to process without spilling to disk. Sizing starts from this expansion factor, not from the raw file size.

---

## The 5-Cores-Per-Executor Guideline

The most widely used Spark baseline is **5 CPU cores per executor**. This is not arbitrary — it reflects three competing pressures:

| Concern | Too few cores per executor | Too many cores per executor |
|---------|---------------------------|----------------------------|
| **HDFS throughput** | Many small executors → high per-connection overhead on NameNode | Not a concern — HDFS handles concurrent reads well |
| **Garbage Collection** | GC pauses happen per executor JVM, so more executors = more GC events in parallel | One large JVM with many threads → long stop-the-world GC pauses |
| **Task parallelism** | Fine — many tasks run concurrently | A single executor running 32 tasks serially loses parallelism |

Five cores sits in the sweet spot: enough parallelism per executor, small enough JVM heap that GC pauses are short, and not so many executors that NameNode connection overhead dominates.

The companion rule is **~1 GB of execution memory per CPU core** before overhead.

---

## Memory Calculation: Step by Step

Given a node with `R` GB of RAM:

1. **Reserve 1 GB for the OS** — the operating system, monitoring agents, and kernel buffers need headroom.
2. **Available RAM per node** = `R - 1`
3. **Divide by executors per node** to get raw memory per executor.
4. **Subtract 7% for JVM and Spark overhead** — this is the `spark.executor.memoryOverhead` reserve Spark uses for JVM internals, shuffle buffers, and off-heap data structures.

```
usable_executor_memory = floor((R - 1) / executors_per_node) * 0.93
```

### Worked Example: 10-Node Cluster, 64 GB RAM

**Cluster specification:**

| Parameter | Value |
|-----------|-------|
| Nodes | 10 |
| CPU cores per node | 16 |
| RAM per node | 64 GB |

**Step 1 — Reserve OS resources:**

```
Available CPU cores per node  = 16 - 1 = 15
Available RAM per node        = 64 - 1 = 63 GB
```

**Step 2 — Executors per node (5-core rule):**

```
Executors per node = 15 / 5 = 3 executors
```

**Step 3 — Raw memory per executor:**

```
Raw memory per executor = 63 / 3 = 21 GB
```

**Step 4 — Subtract 7% overhead:**

```
Usable executor memory = 21 × 0.93 ≈ 19.5 GB  (use 19 GB to be safe)
```

**Step 5 — Total cluster capacity:**

```
Total executors    = 10 nodes × 3 executors = 30 executors
Total CPU cores    = 30 × 5                 = 150 cores
Total usable RAM   = 30 × 19 GB             = 570 GB
```

!!! tip "Rule of thumb summary"
    For a node with N cores and R GB RAM:
    `executors_per_node = (N - 1) / 5`
    `memory_per_executor = ((R - 1) / executors_per_node) × 0.93`

---

## Partition Sizing

### Input Partitions

Spark reads data in partitions. The default target partition size is **128 MB**. For a given input dataset:

```
input_partition_count = ceil(input_data_size_MB / 128)
```

For 100 GB of input data:

```
input_partition_count = ceil(100 × 1024 / 128) = ceil(102400 / 128) = 800 partitions
```

Each partition becomes one task. The number of tasks that run concurrently equals the number of available cores. If you have 150 cores and 800 tasks, the job runs in `ceil(800 / 150) ≈ 6` waves.

### Shuffle Partitions

After a shuffle operation (groupBy, join, window), Spark creates `spark.sql.shuffle.partitions` output partitions. The **default is 200** — which is almost always wrong for production datasets.

**The problem with the default:**

For a 100 GB shuffle:

```
partition_size = 100 GB / 200 = 500 MB per partition
```

With the 4–6× memory expansion factor, a single 500 MB shuffle partition may require 2 GB of executor memory to process — causing spill or OOM on executors with 19 GB of memory that are also holding cached data or processing other partitions concurrently.

**The correct approach — size shuffle partitions to target ~128 MB:**

```
spark.sql.shuffle.partitions = ceil(total_shuffle_data_MB / 128)
```

=== "PySpark"

    ```python
    # Size shuffle partitions to target 128 MB per partition
    # For a 100 GB shuffle:
    spark.conf.set("spark.sql.shuffle.partitions", 800)

    # For a 150 GB join (orders 100 GB + customers 50 GB):
    spark.conf.set("spark.sql.shuffle.partitions", 1200)
    ```

=== "SQL"

    ```sql
    -- Set before running the query
    SET spark.sql.shuffle.partitions = 800;

    SELECT customer_id, SUM(order_amount)
    FROM orders
    GROUP BY customer_id;
    ```

!!! warning "The default 200 is almost always wrong"
    `spark.sql.shuffle.partitions = 200` was designed for demo-sized datasets. In production with hundreds of gigabytes of data, each shuffle partition will be several hundred megabytes — far above the 128 MB target — leading to memory pressure, spill, and slow tasks.

---

## Resource Sizing Scenarios

### Scenario A: 100 GB Aggregation Job

```
Input data:           100 GB
Target partition:     128 MB
Input partitions:     800

To process all 800 partitions in 1 wave:
  Required cores:     800
  Required executors: 800 / 5 = 160

Shuffle partitions:   800 (to hit ~125 MB per partition)
```

**Maximum parallelism option (160 executors):**

| Stage | Partitions | Waves | Notes |
|-------|-----------|-------|-------|
| Read | 800 | 1 | All partitions processed simultaneously |
| Shuffle | 800 | 1 | No waiting; full throughput |

**Moderate footprint option (80 executors, 400 cores):**

| Stage | Partitions | Waves | Partition size |
|-------|-----------|-------|----------------|
| Read | 800 | 2 | 125 MB — healthy |
| Shuffle (400 partitions) | 400 | 1 | 250 MB — slightly large but acceptable |

!!! note "The goal is balance, not maximum"
    Maximum parallelism doubles the cost for a 2× speedup. For batch jobs with multi-hour SLAs, the moderate option is often the right business decision.

### Scenario B: 150 GB Join (Orders + Customers)

```
Orders:              100 GB   →   800 partitions
Customers:            50 GB   →   400 partitions
Total:               150 GB   →  1200 partitions combined

Maximum parallelism:
  Required cores:    1200
  Required executors: 1200 / 5 = 240
  Shuffle partitions: 1200

Optimized (120 executors, 600 cores):
  Stage 1 waves: 1200 / 600 = 2 waves
  Shuffle: 600 partitions → 150 GB / 600 = 250 MB/partition
```

---

## Dynamic Resource Allocation

Static allocation holds the same executor count through every stage of a job. A pipeline with a heavy shuffle stage followed by a light aggregation stage wastes resources during the lighter stages.

**Dynamic Resource Allocation** lets Spark scale executors up and down based on actual task backlog.

```python
spark.conf.set("spark.dynamicAllocation.enabled", True)
spark.conf.set("spark.dynamicAllocation.minExecutors", 10)
spark.conf.set("spark.dynamicAllocation.initialExecutors", 20)
spark.conf.set("spark.dynamicAllocation.maxExecutors", 200)
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

| Parameter | Effect |
|-----------|--------|
| `minExecutors` | Floor — never go below this; avoids cold-start latency |
| `initialExecutors` | Executors requested at job start |
| `maxExecutors` | Cap — prevents runaway cost |
| `schedulerBacklogTimeout` | How long a task must be pending before Spark requests more executors |
| `executorIdleTimeout` | How long an executor sits idle before Spark releases it |

!!! tip "When to use Dynamic Resource Allocation"
    Use DRA for jobs with **variable-stage workloads** — e.g., pipelines that ingest a small daily delta but then run a large historical backfill join. For simple single-stage ETL jobs with predictable data volumes, static allocation with pre-calculated executor counts is more predictable and avoids executor acquisition latency.

---

## Anti-Patterns

### Too Many Small Executors

```
Bad:  1 core per executor, 300 executors on a 10-node cluster
Why:  Each executor is a separate JVM process.
      300 JVMs × per-JVM overhead = significant memory waste.
      300 concurrent HDFS connections = NameNode pressure.
      GC events happen 300 times in parallel, degrading throughput.
```

### Too Few Large Executors

```
Bad:  1 executor per node with all 15 cores
Why:  One JVM holds 63 GB of heap.
      A single stop-the-world GC pause freezes all 15 tasks.
      No task-level isolation — a skewed partition starves all other tasks.
      HDFS recommends at most 5 concurrent threads per executor for optimal throughput.
```

### Ignoring the Shuffle Partition Default

```
Bad:  100 GB dataset, spark.sql.shuffle.partitions = 200
      → 500 MB per partition → 2 GB in-memory → spill → 10× slower
Good: spark.sql.shuffle.partitions = 800
      → 125 MB per partition → 500 MB in-memory → no spill
```

### Calling `.collect()` on Large DataFrames

```python
# This pulls the entire dataset into driver memory
data = df.collect()  # DO NOT do this on a 100 GB DataFrame

# Keep processing distributed; only collect small result sets
summary = df.groupBy("status").count().collect()  # Fine — small result
```

---

## Cost Estimation Mental Model

| Resource | Pricing Unit | Typical Range |
|----------|-------------|---------------|
| **Databricks** | DBU (Databricks Unit) per hour per node | Jobs Compute: ~0.07–0.15 DBU/hr per node |
| **AWS EMR** | EC2 instance-hours + EMR markup (~25%) | Varies by instance type |
| **Azure HDInsight** | VM cost + HDInsight markup | Varies by VM SKU |
| **CPU hours** | executor_count × cores_per_executor × wall_clock_hours | Direct function of parallelism |
| **Memory hours** | executor_count × memory_per_executor_GB × wall_clock_hours | Drives memory-intensive cost |

**Rule of thumb for cost estimation:**

```
job_cost ≈ executor_count × (cores × cpu_rate + memory_GB × mem_rate) × duration_hours
```

For cloud-managed services, more executors running for less time costs roughly the same as fewer executors running longer — but **staying within memory** (no spill) is often 5–10× faster at the same resource level because disk spill adds I/O that consumes wall-clock time without adding useful work.
