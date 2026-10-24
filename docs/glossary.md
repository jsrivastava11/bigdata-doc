# Glossary

Key terms used across the Big Data & Cloud Engineering Guide — from distributed storage to streaming, cloud platforms, and interview preparation. Use this as a quick reference when you encounter unfamiliar jargon.

---

## A

**ACID**
Atomicity, Consistency, Isolation, Durability — the four guarantees that make a write operation database-grade. Delta Lake brings ACID semantics to data lake storage: writes either fully commit or fully roll back, concurrent readers always see a consistent snapshot, and committed data survives node failures.

**ADLS Gen2 (Azure Data Lake Storage Gen2)**
Microsoft's enterprise analytics storage service, built on Azure Blob Storage with the Hierarchical Namespace (HNS) enabled. Provides real directory trees, atomic O(1) rename/move operations, and POSIX-style ACLs at the file and directory level. The correct storage choice for Spark and Databricks workloads on Azure.

**AQE (Adaptive Query Execution)**
A Spark optimization framework (enabled by default in Spark 3.2+) that re-optimizes the physical query plan at runtime using actual shuffle statistics. Three main features: dynamically coalescing small shuffle partitions, dynamically switching join strategies, and dynamically splitting skewed partitions in joins.

**Auto Loader**
A Databricks file ingestion utility that incrementally processes new files arriving in cloud storage (ADLS Gen2, S3, GCS). Uses file notifications or directory listing to detect new files without scanning the entire directory. The recommended way to ingest streaming file data into Delta Lake.

**Availability Zone**
A physically separate datacenter within an Azure or AWS region, with independent power, cooling, and networking. Deploying across multiple AZs protects workloads from single-datacenter failures. Azure ZRS (Zone-Redundant Storage) replicates data synchronously across three AZs.

**Avro**
A row-based binary serialization format developed by the Apache Hadoop project. Stores schema alongside data in a header, making it self-describing. Popular for Kafka messages because the compact binary format reduces network overhead. Less suited for analytical column scans than Parquet.

**AWS Glue**
A fully managed serverless ETL service on AWS. Provides a visual ETL interface (Glue Studio), a Python/Scala Spark runtime, a centralized Data Catalog (Hive-compatible metastore), and built-in data quality via DQDL rulesets. Commonly used alongside EMR and Athena in AWS data lake architectures.

---

## B

**Batch Processing**
A data processing paradigm that collects data over a period (hours, days) and processes it in a single scheduled job. Optimized for throughput rather than latency. Examples: nightly ETL jobs, daily aggregation pipelines. Contrasts with stream processing, which processes data continuously as it arrives.

**Bloom Filter**
A space-efficient probabilistic data structure that tests whether an element is definitely not in a set, or is possibly in the set. False negatives are impossible; false positives are possible. Used in Spark's runtime Bloom filter join optimization and Delta Lake's column-skip statistics to avoid unnecessary file reads.

**Broadcast Join**
A Spark join strategy where the smaller table is copied (broadcast) to every executor in memory, and the larger table is scanned once locally on each executor. Eliminates the shuffle entirely, making it the most efficient join strategy when the smaller table fits in memory. Default threshold: 10 MB (tunable to ~200 MB).

**Bronze Layer**
The first zone in the Medallion architecture. Contains raw, unmodified data exactly as it arrived from the source system. Append-only and immutable. Acts as a historical replay buffer — if downstream processing has a bug, Silver and Gold can be re-derived from Bronze without re-ingesting from the source.

**Bucketing**
A Spark optimization that pre-sorts and distributes data into a fixed number of files (buckets) by a hash of a designated column. Unlike partitioning (which creates directories), bucketing creates a fixed number of files per partition. Eliminates the shuffle for joins and groupBy operations on the bucketed column when both sides are bucketed on the same key with the same bucket count.

---

## C

**CAP Theorem**
A theorem stating that a distributed data system can guarantee at most two of three properties: Consistency (every read returns the latest write), Availability (every request receives a response), and Partition Tolerance (the system continues operating during network partitions). Since partitions are a physical reality, the real choice is CP (consistent but may return errors) vs. AP (available but may return stale data).

**CDF (Change Data Feed)**
A Delta Lake feature that records row-level changes (inserts, updates, deletes) to a table in a special `_change_data` directory. Consumers can read only the incremental changes since their last read, enabling efficient downstream propagation without full table scans.

**Checkpoint**
In Spark Structured Streaming, a directory in durable storage (ADLS, S3) that records the last successfully processed offset from the source (Kafka, Event Hubs) and the current streaming query state. Enables exact resumption after failure without reprocessing already-committed data. Also used in Delta Lake to periodically consolidate the transaction log.

**Cluster Manager**
The component responsible for allocating resources across a Spark cluster. Common cluster managers: YARN (Hadoop), Kubernetes, Mesos, and Spark Standalone. In Databricks, the cluster manager is managed by the platform — you specify the number and type of workers rather than configuring the scheduler directly.

**Columnar Storage**
A storage layout where each column's values are stored contiguously on disk, rather than storing full rows together. Enables efficient analytical queries that read only relevant columns (column pruning) and apply encodings (dictionary, run-length) per-column for better compression. Parquet and ORC are columnar formats; Avro and CSV are row-based.

**Combiner**
In MapReduce, an optional local aggregation step that runs on each mapper's output before data is sent to the reducer. Reduces the volume of data shuffled across the network. The Spark equivalent is `reduceByKey()` or `aggregateByKey()`, which perform partial aggregation per partition before the shuffle, unlike `groupByKey()` which skips this step.

**Consumer Group**
A set of Kafka consumers that jointly consume a topic. Kafka guarantees that each partition is consumed by exactly one consumer within a group at any time, enabling parallelism proportional to the number of partitions. Multiple independent consumer groups can consume the same topic simultaneously from different offsets.

**COPY INTO**
A Databricks SQL and Delta Lake command for loading files from cloud storage into a Delta table. Idempotent by design — tracks which files have already been loaded and skips them on subsequent runs. Simpler than Auto Loader for one-time or infrequent bulk loads.

---

## D

**DAG (Directed Acyclic Graph)**
The execution plan that Spark builds from a sequence of transformations. Each node in the DAG is an RDD or DataFrame partition; edges represent dependencies. The DAG is partitioned into stages at wide transformation boundaries (shuffle points). Viewing the DAG in the Spark UI is the primary tool for diagnosing performance issues.

**Data Lake**
A storage repository that holds a large volume of raw data in its native format (structured, semi-structured, unstructured) until needed. Typically backed by cheap object storage (S3, ADLS Gen2). Early data lakes lacked ACID transactions and governance, which the data lakehouse pattern addresses.

**Data Lakehouse**
An architectural pattern combining the low-cost scalable storage of a data lake with the ACID transactions, schema enforcement, and query performance of a data warehouse. Delta Lake is the storage layer that makes this possible. Pioneered by Databricks.

**DataNode**
In HDFS, the worker node that stores actual data blocks and serves read/write requests from clients. Reports its block inventory to the NameNode via heartbeat. Typically runs on the same physical machines as YARN NodeManagers to enable data locality in Spark and MapReduce.

**DBU (Databricks Unit)**
The unit of processing capability used to calculate Databricks platform costs, separate from the underlying cloud VM cost. DBU consumption varies by compute type (All-Purpose, Jobs, SQL Warehouse, Serverless) and instance size. Used to estimate and optimize Databricks spending.

**Delta Lake**
An open-source storage layer that adds ACID transactions, schema enforcement, time travel (versioned query history), and audit history to Parquet files on cloud storage. The default table format in Databricks and the foundation of the Lakehouse architecture.

**Delta Transaction Log**
An ordered journal of every operation performed on a Delta table, stored in the `_delta_log/` directory alongside the Parquet data files. Each commit is an atomic JSON file listing files added/removed, the operation type, statistics, and schema. Enables ACID semantics, time travel, and concurrent write conflict detection.

**Dimensional Modeling**
A data warehouse design methodology (popularized by Ralph Kimball) that organizes data into fact tables (measurable events) and dimension tables (descriptive context). The star schema is the primary pattern: a central fact table joined to surrounding dimension tables. Optimized for BI query performance.

**DynamicFrame**
An AWS Glue-specific data abstraction, similar to a Spark DataFrame but with additional schema flexibility for semi-structured data (inconsistent types, missing fields). DynamicFrames can be converted to Spark DataFrames for standard Spark operations. Used in Glue ETL scripts for handling messy source data.

---

## E

**EMR (Amazon EMR)**
Amazon's managed big data platform that runs Apache Spark, Hadoop, Hive, Presto, and other open-source frameworks on EC2 clusters. Supports Instance Groups (fixed capacity) and Instance Fleets (Spot + On-Demand mix). The AWS counterpart to Azure Databricks for running Spark workloads.

**Executor**
A JVM process launched on a Spark worker node for a specific application. Executors run tasks, store cached RDD/DataFrame partitions in memory, and write shuffle output files. Each executor has a fixed number of CPU cores and a memory allocation. When an executor dies, Spark re-schedules its tasks on other executors and re-computes any lost partitions from lineage.

---

## F

**Fact Table**
In dimensional modeling, a table that stores measurable, quantitative business events (sales transactions, page views, sensor readings). Contains foreign keys to dimension tables and numeric measures. Typically the largest table in a data warehouse. Examples: `fact_orders`, `fact_page_views`.

**Fault Tolerance**
The ability of a system to continue operating correctly after one or more component failures. In Spark, fault tolerance is achieved through DAG lineage (re-compute lost partitions) and checkpointing (persist state to durable storage). In Kafka, fault tolerance is achieved through partition replication across multiple brokers.

---

## H

**HDFS (Hadoop Distributed File System)**
The distributed file system that underpins the Hadoop ecosystem. Files are split into 128 MB blocks and stored with a replication factor (default 3) across DataNodes. Provides fault tolerance through replication and enables data locality for MapReduce and Spark workloads. Largely replaced by cloud object storage (S3, ADLS Gen2) in modern architectures.

**Hive Metastore**
A relational database (typically MySQL or PostgreSQL) that stores metadata about Hive tables: schema definitions, partition locations, file formats, and storage paths. Used by Spark, Presto, Hive, and other engines to discover and query tables without knowing their physical location. In Databricks, the workspace-level Hive Metastore is being superseded by Unity Catalog.

**Horizontal Scaling**
Increasing capacity by adding more nodes (machines) to a cluster rather than upgrading existing machines. The primary scaling model for distributed data systems like Spark, Kafka, and HDFS. Contrasts with vertical scaling (adding CPU/RAM to existing nodes). Horizontal scaling is preferred for large data workloads because it is theoretically unbounded and does not require downtime.

---

## I

**Idempotency**
The property of an operation that produces the same result regardless of how many times it is applied. Critical in data pipelines because jobs are retried on failure — without idempotency, retries cause duplicate rows or corrupted aggregates. Achieved by using MERGE INTO on a natural key, COPY INTO (which tracks processed files), or INSERT OVERWRITE for full partition replacement.

**Integration Runtime (IR)**
The compute infrastructure and connectivity component in Azure Data Factory that executes activities and moves data. Three types: Azure IR (managed cloud, cloud-to-cloud), Self-Hosted IR (installed on-premises or in a private network for hybrid connectivity), and Azure-SSIS IR (for running legacy SSIS packages in Azure).

**ISR (In-Sync Replicas)**
The set of Kafka partition replicas that are fully caught up to the leader broker. A producer with `acks=all` waits for all ISR members to acknowledge a write before considering it successful. If a replica falls behind (exceeds `replica.lag.time.max.ms`), it is removed from the ISR until it catches up.

---

## K

**Kafka Broker**
A server in a Kafka cluster that stores topic partitions and serves producer and consumer requests. Each partition has one leader broker (handles all reads/writes) and zero or more follower replicas (ISR members) on other brokers. A Kafka cluster typically runs 3, 5, or 7 brokers for fault tolerance.

**Kafka Consumer**
A client application that reads messages from one or more Kafka topic partitions. Consumers in the same consumer group divide the partitions among themselves. Each consumer tracks its read position using an offset committed back to Kafka (or managed by the consumer application).

**Kafka Partition**
The fundamental unit of parallelism and ordering in Kafka. A topic is divided into one or more partitions, each of which is an ordered, immutable sequence of messages. Messages within a partition are ordered and assigned a monotonically increasing offset. Partitions are distributed across brokers for load balancing and replicated for fault tolerance.

**Kafka Producer**
A client application that publishes messages to Kafka topics. Producers choose which partition to write to — either by round-robin (load balancing) or by a partition key (ordering guarantee). The `acks` setting controls how many broker acknowledgements the producer requires before considering a write successful.

**Kafka Topic**
A named, durable, ordered stream of messages in Kafka. Topics are divided into partitions for parallelism and replicated across brokers for fault tolerance. Topics retain messages for a configurable retention period (time-based or size-based) or indefinitely with log compaction.

---

## L

**Lambda Architecture**
A data processing architecture that maintains two parallel processing paths: a batch layer (accurate, reprocesses historical data) and a speed layer (low-latency, processes recent data). Results are merged at query time. The complexity of maintaining two codebases has led to the Kappa architecture (streaming only) and the Lakehouse architecture (streaming + batch on the same Delta table) superseding it in modern DE.

**Lazy Evaluation**
Spark's execution model where transformations are recorded as a DAG but not executed until an action is called. Enables full plan optimization (predicate pushdown, column pruning, join reordering) before any computation runs, and allows short-circuit evaluation (e.g., `limit()` stops reading as soon as enough rows are found).

**Lineage**
The tracking of where data came from and how it was transformed. Unity Catalog provides automatic end-to-end lineage graphs (source table → transformation → downstream table or dashboard) without manual annotation. Critical for impact analysis (what breaks if I change this table?) and regulatory data governance.

**Log Compaction**
A Kafka retention policy (`cleanup.policy=compact`) that retains only the most recent message for each key in a partition. Older messages with the same key are deleted asynchronously. Allows new consumers to reconstruct the current state of keyed data by replaying the compacted log rather than the full event history.

---

## M

**MapReduce**
A distributed batch processing paradigm where computation is divided into a map phase (transform each input record) and a reduce phase (aggregate results by key). Introduced by Google (2004) and implemented in Apache Hadoop. Largely superseded by Spark, which avoids writing intermediate results to disk between stages.

**Medallion Architecture**
A layered data design pattern with three zones: Bronze (raw ingested data), Silver (cleaned and validated data), and Gold (business-ready aggregated data). Recommended by Databricks as the standard data organization pattern for lakehouses. Enforces separation of concerns, enables independent replay of each layer, and provides stable, high-quality interfaces for downstream consumers.

**Micro-Batch**
The default processing model in Spark Structured Streaming. New data is accumulated over a trigger interval (e.g., every 10 seconds) and processed as a small batch. Balances latency (seconds to minutes) with throughput. Contrasts with continuous processing (sub-millisecond latency) which is available as an experimental mode.

---

## N

**NameNode**
The master node in HDFS that manages the filesystem namespace: tracks which blocks belong to which file, where each block replica is stored, and the health of DataNodes (via heartbeats). A single point of failure in early Hadoop — mitigated by High Availability (HA) NameNode configurations with Active/Standby failover.

---

## O

**OLAP (Online Analytical Processing)**
A data processing category optimized for complex analytical queries over large historical datasets — aggregations, drill-downs, time-series analysis. Characterized by low write frequency, high read volume, and columnar storage formats. Examples: Databricks SQL, Redshift, Snowflake, BigQuery.

**OLTP (Online Transaction Processing)**
A data processing category optimized for high-throughput, low-latency transactional operations (INSERT, UPDATE, DELETE) on row-level data. Characterized by high write frequency, short transactions, and row-based storage formats. Examples: PostgreSQL, MySQL, Oracle, SQL Server.

---

## P

**Parquet**
An open-source columnar binary file format optimized for analytical workloads. Stores data by column, enabling efficient column pruning (read only queried columns) and predicate pushdown (skip row groups using min/max statistics). Supports nested data types and multiple compression codecs (Snappy, ZSTD, GZIP). The default file format for Delta Lake and most modern data lakehouses.

**Partition**
In Spark: a logical division of a DataFrame or RDD. Each partition is processed by one task on one executor. Partition count determines parallelism. In Hive/Delta: a physical directory subdivision of a table based on a column value (e.g., `event_date=2024-06-01/`), enabling partition pruning — only directories matching the filter are read.

**Partition Pruning**
A query optimization where the engine skips reading partitions that cannot match the query predicate. Requires the filter to be on the partition column. Example: `SELECT * FROM orders WHERE order_date = '2024-06-01'` reads only the `order_date=2024-06-01/` directory, ignoring all other partitions.

**Photon**
Databricks' vectorized query engine written in C++. A drop-in replacement for the standard Spark JVM query engine that delivers significant speedups for SQL-heavy workloads through SIMD vectorization and optimized memory access patterns. Enabled at the cluster level with no code changes required.

**PySpark**
The Python API for Apache Spark. Allows Python developers to write Spark jobs using DataFrames, SQL, and Structured Streaming without writing Scala or Java. PySpark serializes Python UDFs to the JVM via Py4J; pandas UDFs (Arrow-based) significantly reduce this serialization overhead for Python-heavy transformations.

---

## R

**RDD (Resilient Distributed Dataset)**
Spark's fundamental low-level data abstraction: an immutable, partitioned collection of elements distributed across a cluster. Provides fault tolerance through lineage tracking. In most production code, the higher-level DataFrame API is preferred because it goes through the Catalyst optimizer; RDDs bypass optimization.

**Redshift Spectrum**
An Amazon Redshift feature that allows Redshift queries to read data directly from S3 (in Parquet, ORC, CSV, JSON formats) without loading it into Redshift storage. Uses a fleet of managed compute nodes separate from the Redshift cluster. Enables querying a data lake through standard SQL without data movement.

**Replication Factor**
The number of copies of a data block or Kafka partition stored across different nodes. HDFS default is 3 (one primary + two replicas). Kafka default is 3. A higher replication factor increases fault tolerance but consumes proportionally more storage.

**Resource Manager**
In Hadoop YARN, the master process that manages cluster resources and schedules applications. Receives resource requests from ApplicationMasters and allocates NodeManager containers. Equivalent to the cluster manager role in Spark Standalone or Kubernetes deployments.

**Row Group**
The primary unit of parallelism in Parquet files. A Parquet file is divided into row groups (default 128 MB), each containing column chunks for all columns. Statistics (min, max, null count) are stored per row group and used by query engines for predicate pushdown — skipping row groups that cannot contain matching values.

---

## S

**S3 (Amazon Simple Storage Service)**
AWS's object storage service. The primary storage layer for AWS-based data lakehouses. Provides 11 nines (99.999999999%) durability, multiple storage classes (Standard, Standard-IA, Glacier), lifecycle rules for automated tier transitions, and event notifications for file-arrival triggers.

**SAS Token (Shared Access Signature)**
A time-limited, permission-scoped URL-based credential for Azure Storage. Grants specific permissions (read, write, delete) on specific resources (container, blob, file share) for a defined validity period. Used to grant temporary third-party access without exposing the full storage account key.

**SCD (Slowly Changing Dimension)**
A dimension table that changes over time. SCD types define how changes are handled: Type 0 (never change), Type 1 (overwrite — no history), Type 2 (add new row with effective dates — full history), Type 3 (add column for previous value — limited history). SCD Type 2 is the most common in enterprise data warehouses.

**Schema Enforcement**
Delta Lake's default behavior of rejecting writes that do not match the declared table schema. Protects downstream consumers from unexpected column additions, type changes, or missing columns from upstream source changes. Override with `mergeSchema=true` to allow additive schema evolution.

**Schema Evolution**
The ability to handle upstream schema changes (new columns, type changes) without breaking downstream pipelines. Strategies include mergeSchema (accept additive changes), schema-on-read (Bronze absorbs raw data without enforcement), and explicit schema registry governance for Kafka-based pipelines.

**Serverless SQL Pool**
An Azure Synapse Analytics compute tier that queries data directly in ADLS Gen2 using T-SQL without provisioning a dedicated cluster. Pay-per-query based on data scanned. Best for ad-hoc exploration of raw files (Parquet, Delta, CSV) in a data lake rather than high-concurrency BI workloads.

**Shuffle**
A wide transformation in Spark that requires redistributing data across all executors — all data for a given key must be co-located on the same executor. Examples: `groupBy()`, `join()`, `distinct()`, `repartition()`. Shuffles involve writing data to disk and transferring it across the network — the most expensive operation in a Spark job. Reducing shuffle count and size is the primary focus of Spark performance optimization.

**Sort-Merge Join**
Spark's default join strategy for large-large table joins. Both tables are first shuffled by the join key to co-locate matching partitions, then each co-located partition pair is sorted and merged. Requires O(N log N) time but is robust for datasets of any size. AQE can convert sort-merge joins to broadcast joins at runtime if one side turns out to be small.

**SparkSession**
The unified entry point for Spark programming since Spark 2.0. Encapsulates SparkContext, SQLContext, and HiveContext into a single object. Used to create DataFrames, register temporary views, run SQL queries, read from and write to various data sources, and configure Spark properties.

**SQL Warehouse**
The compute engine powering Databricks SQL workloads. Available in Classic, Pro, and Serverless tiers. Serverless SQL Warehouses start in under 5 seconds, scale automatically, and charge per second of use. Designed for SQL-fluent analysts running BI queries against Unity Catalog tables.

**Star Schema**
A dimensional modeling pattern where a central fact table is surrounded by denormalized dimension tables connected by foreign keys. Resembles a star when drawn. Optimized for analytical query performance — joins are simple and predictable. The most common data warehouse design pattern.

**Storage Level**
In Spark, the configuration that controls where cached RDD or DataFrame data is stored: `MEMORY_ONLY` (JVM heap), `MEMORY_AND_DISK` (spill to disk if memory is insufficient), `MEMORY_ONLY_SER` (serialized, more compact), `DISK_ONLY`, or `OFF_HEAP`. Choose based on available memory and whether recomputation is cheap.

**Structured Streaming**
Spark's stream processing engine built on the DataFrame API. Treats a data stream as an unbounded table that is continuously appended to. Supports micro-batch and continuous processing modes, stateful aggregations, watermarking for late data, and exactly-once semantics with Delta Lake as the sink.

---

## T

**Time Travel**
A Delta Lake feature that allows querying a table as it existed at a previous version number or timestamp: `SELECT * FROM table VERSION AS OF 42` or `TIMESTAMP AS OF '2024-06-01'`. Data is available until `VACUUM` removes files older than the retention threshold. Used for debugging, auditing, ML reproducibility, and rollback.

**Tumbling Window**
A non-overlapping, fixed-size time window used in streaming aggregations. Each event belongs to exactly one window. Example: aggregate page views per 5-minute window. In ADF, a Tumbling Window Trigger processes fixed, non-overlapping time windows with native backfill support — useful for reprocessing historical intervals.

---

## U

**UNION ALL**
A SQL operator that concatenates the rows of two or more SELECT statements without removing duplicates. Faster than `UNION` (which performs deduplication) because it requires no sort or hash. The correct choice when combining data from multiple sources where duplicates are either impossible or acceptable.

**Unity Catalog**
Databricks' unified governance layer for all data and AI assets, attached at the account level. Provides a three-level namespace (`catalog.schema.table`) shared across all workspaces, fine-grained access control (column-level, row-level), automatic lineage tracking, centralized audit logs, and built-in Delta Sharing.

---

## V

**VACUUM**
A Delta Lake maintenance command that deletes Parquet files no longer referenced by the transaction log and older than the retention threshold (default 168 hours / 7 days). Frees storage occupied by files left behind by MERGE, UPDATE, DELETE, and OPTIMIZE operations. Permanently removes data — cannot be undone.

**Vertical Scaling**
Increasing capacity by upgrading existing machines (more CPU, more RAM, faster SSD) rather than adding new nodes. Limited by physical hardware maximums and typically requires downtime. Used for single-node systems (OLTP databases) and Spark driver nodes. Contrasts with horizontal scaling, which is the preferred approach for distributed data workloads.

---

## W

**Watermark**
In Spark Structured Streaming, a threshold that defines how long the engine waits for late-arriving events before closing a time window and evicting its state from memory. Example: `withWatermark("event_time", "5 minutes")` tells Spark to accept events up to 5 minutes late, then discard state for older windows. Essential for bounded state in stateful streaming aggregations.

**Wide Transformation**
A Spark transformation that requires a shuffle — data must move between executors because rows from different partitions are needed to compute the result. Examples: `groupBy()`, `join()`, `distinct()`, `repartition()`. Wide transformations create stage boundaries in the DAG. Contrasts with narrow transformations (filter, map, select) where each output partition depends on at most one input partition.

**Window Function**
A SQL/Spark function that computes a result for each row using a set of related rows defined by a window specification (PARTITION BY + ORDER BY + frame). Unlike GROUP BY (which collapses rows), window functions return one row per input row. Examples: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `SUM() OVER (ORDER BY ...)`.

**Worker Node**
A machine in a distributed cluster that runs executor processes (Spark) or DataNodes/NodeManagers (Hadoop). Worker nodes perform the actual data processing tasks assigned by the driver or cluster manager. Adding worker nodes (horizontal scaling) increases the cluster's total processing capacity and data storage.

---

## Y

**YARN (Yet Another Resource Negotiator)**
Hadoop's cluster resource management layer. Separates resource management (YARN ResourceManager + NodeManagers) from programming models (MapReduce, Spark, Hive). The ResourceManager allocates containers to ApplicationMasters, which in turn request executor containers on behalf of their Spark jobs. Spark can also run on Kubernetes, Mesos, and Standalone without YARN.

---

## Z

**Z-Ordering**
A Delta Lake file layout optimization that co-locates related data within the same set of Parquet files by interleaving the sort orders of multiple columns. Enables multi-dimensional data skipping — when a query filters on any Z-Ordered column, Delta can skip a higher fraction of files than single-column ordering allows. Applied via `OPTIMIZE table ZORDER BY (col1, col2)`.

---

## Additional Terms

**ADF (Azure Data Factory)**
Microsoft's cloud-based serverless data integration and orchestration service. Used to build automated ETL/ELT pipelines that copy data between 90+ sources and sinks, execute Spark transformations via Mapping Data Flows, trigger Databricks notebooks, and manage pipeline dependencies, retries, scheduling, and alerting.

**Broadcast Variable**
In Spark, a read-only variable cached on every executor, sent to each executor once at the start of a job rather than serialized with every task. Used for large lookup dictionaries or configuration maps that are referenced by many tasks. The DataFrame `broadcast()` hint applies the same principle to join optimization.

**Bronze / Silver / Gold**
See *Medallion Architecture*. The three layers of the Medallion pattern. Bronze is raw and immutable, Silver is cleaned and validated, Gold is business-ready and aggregated.

**CDC (Change Data Capture)**
A technique for capturing row-level changes (inserts, updates, deletes) from a source database and propagating them to a downstream system. Log-based CDC (e.g., Debezium reading PostgreSQL WAL) is the preferred approach — it reads the database transaction log rather than polling tables, which avoids performance impact on the source system.

**Debezium**
An open-source CDC platform that reads database transaction logs (PostgreSQL WAL, MySQL binlog, SQL Server CDC) and publishes change events to Kafka topics. Each event includes the operation type (`c`=create, `u`=update, `d`=delete) and before/after row states. The standard tool for log-based CDC pipelines.

**DIU (Data Integration Unit)**
The unit of compute capacity allocated to an ADF Copy Activity. Higher DIU settings provide more parallel network connections and compute resources for data movement. Tuning DIUs is one of the primary levers for improving slow ADF Copy Activity performance.

**Driver**
The JVM process that coordinates a Spark application: parses the user program, builds the DAG, divides the DAG into stages and tasks, schedules tasks on executors via the cluster manager, and collects results. The driver runs on the Spark master node (or a dedicated driver node in Databricks). Driver memory must be large enough to hold the final collected result and broadcast variables.

**Event Hub**
Microsoft Azure's managed event streaming service. Provides a Kafka-compatible API, allowing Kafka producers and consumers to connect without code changes. Supports up to 32 partitions per namespace on the standard tier. Commonly used as the message broker in Azure-based streaming pipelines.

**Foreign Key**
In dimensional modeling, a column in a fact table that references the primary key of a dimension table. Used to join the fact table to its dimensions during analytical queries. Foreign key constraints are typically not enforced in data warehouse fact tables for performance reasons, but referential integrity is validated as a data quality check.

**Grain**
In dimensional modeling, the grain defines the level of detail captured by each row in a fact table. Declaring the grain is the second step in Kimball's four-step dimensional design process (after identifying the business process). Example: "one row per order line item" vs. "one row per daily order total per customer." Mixing grains in the same fact table is a common data modeling mistake.

**Hadoop**
The open-source framework that introduced distributed storage (HDFS) and distributed processing (MapReduce) at scale. Apache Spark was originally built on top of Hadoop and YARN. Modern cloud data platforms (Databricks, EMR, Synapse) have largely replaced on-premises Hadoop clusters, but HDFS architecture concepts remain foundational for understanding distributed data systems.

**Hot / Cool / Cold / Archive**
Azure Blob Storage and ADLS Gen2 access tiers for cost optimization. Hot: highest storage cost, lowest access cost — for frequently accessed data. Cool: lower storage cost, higher access cost — for infrequent access (once per month). Cold: even lower storage cost — for rare access (once per 90 days). Archive: lowest storage cost, highest rehydration cost and latency (hours) — for long-term retention.

**Iceberg**
An open-source table format (Apache project) that provides ACID transactions, schema evolution, partition evolution, and time travel on data lake storage. Functionally similar to Delta Lake but with stronger multi-engine portability (works natively with Spark, Flink, Trino, Hive). Delta Lake is typically preferred within the Databricks ecosystem.

**Integration Runtime**
See entry under *I* in the main glossary.

**Key Vault (Azure Key Vault)**
Azure's managed secrets management service. Stores and controls access to sensitive credentials: database passwords, storage account keys, SAS tokens, encryption keys, and certificates. Databricks Secret Scopes backed by Key Vault allow notebooks and pipelines to retrieve secrets at runtime without hardcoding them.

**Linked Service**
An ADF object that stores the connection and authentication information for a data store or compute resource. Similar to a connection string, but managed centrally in ADF. A pipeline activity references a Linked Service to know how to connect to the source or sink. Example: an ADLS Gen2 Linked Service stores the storage account name and authentication method.

**LRS / ZRS / GRS / RA-GZRS**
Azure Storage redundancy levels. LRS (Locally Redundant Storage): 3 copies within one datacenter. ZRS (Zone-Redundant Storage): 3 copies across 3 availability zones in one region. GRS (Geo-Redundant Storage): 6 copies — 3 in primary region (LRS) + 3 in a secondary region. RA-GZRS: read access from the secondary region combined with zone redundancy in the primary.

**MPP (Massively Parallel Processing)**
An architecture where a query is distributed across many independent nodes, each processing a subset of the data simultaneously. Used in data warehouse engines like Azure Synapse Dedicated SQL Pool, Amazon Redshift, and Snowflake. Each node has its own CPU, memory, and storage — no shared resources. Enables query performance that scales linearly with the number of nodes.

**Natural Key**
In dimensional modeling, the business identifier that uniquely identifies a dimension member in the source system (e.g., customer email, product SKU, employee ID). Natural keys are used for matching and deduplication in CDC and SCD implementations. Contrasts with surrogate keys, which are system-generated integers used as the primary key in the data warehouse.

**Offset (Kafka)**
A monotonically increasing integer that uniquely identifies each message within a Kafka partition. Consumers use offsets to track their position in a partition — committing an offset means "I have processed all messages up to this position." Spark Structured Streaming stores Kafka offsets in checkpoint files to enable exactly-once resumption after failure.

**ORC (Optimized Row Columnar)**
A columnar binary file format developed for the Hadoop ecosystem, similar to Parquet. Supports predicate pushdown and compression. Preferred by Hive; Parquet is preferred by Spark and the broader cloud data lake ecosystem.

**Parquet Predicate Pushdown**
A query optimization where filters are applied at the Parquet file reader level before data is deserialized into Spark rows. Parquet stores min/max statistics per row group — the reader skips row groups that cannot contain matching values without reading them. Dramatically reduces I/O for selective queries on large tables.

**Salting**
A technique for resolving data skew in Spark joins. Append a random integer (the "salt") to skewed keys on the large side of the join, then explode the smaller side to have one row per salt value for each key. The join then distributes the skewed data across multiple partitions. After the join, the salt column is dropped.

**Serverless Compute**
A compute model where the platform automatically provisions, scales, and terminates infrastructure — you pay for usage rather than reserved capacity. In Databricks: Serverless SQL Warehouses (start in under 5 seconds), Serverless Jobs (no cluster to configure), and Serverless DLT pipelines. Eliminates cluster management overhead and idle cost.

**Skew**
An uneven distribution of data across partitions in Spark. When most rows for a join or aggregation key land in the same partition, the task processing that partition takes 10–100× longer than other tasks — the straggler task. Detected by inspecting stage completion times in the Spark UI. Resolved by broadcast joins, salting, or AQE skew join optimization.

**Streaming Join**
Joining two streaming DataFrames in Spark Structured Streaming. Requires defining a watermark on both sides to bound the state that Spark must maintain. Without a watermark, the engine must retain all records from both streams indefinitely (unbounded state). Streaming-static joins (stream + Delta table) do not require a watermark on the static side.

**Surrogate Key**
A system-generated integer primary key used in data warehouse dimension tables, independent of the business key. Surrogate keys are stable (they don't change when the natural key changes), compact (integer vs. long string), and necessary for SCD Type-2 implementations where multiple rows represent different versions of the same business entity.

**Task**
The smallest unit of work in Spark. Each stage is divided into tasks — one task per partition. Tasks run in parallel on executor cores. A Spark job with 1,000 partitions in the final stage spawns 1,000 tasks. Task duration variance in the Spark UI is the primary signal for diagnosing data skew and straggler tasks.

**Trigger (Spark SS)**
The interval at which Spark Structured Streaming processes new data. Options: `processingTime="10 seconds"` (micro-batch on a timer), `once` (process all available data and stop — batch mode), `availableNow` (process all available data across multiple micro-batches, then stop), `continuous` (experimental sub-millisecond processing mode). Choose based on latency requirements and throughput.

**Trigger (ADF)**
A mechanism in Azure Data Factory that initiates pipeline execution. Types: Schedule Trigger (calendar-based, e.g., every day at 5 AM), Tumbling Window Trigger (fixed non-overlapping time windows with backfill support), Event-Based Trigger (fires when a file arrives in ADLS or Blob Storage), and Storage Event Trigger.

**Unity Catalog Namespace**
The three-level naming scheme used by Unity Catalog: `catalog.schema.table`. Catalogs are the top-level boundary (e.g., `prod`, `dev`), schemas group related tables within a catalog (e.g., `sales`, `marketing`), and tables are the leaf objects. All standard SQL DDL and DML statements use this fully-qualified naming convention.

**WAL (Write-Ahead Log)**
A durable log of operations written to disk before the operation is applied. Used by Spark Structured Streaming (in legacy `updateStateByKey`) and by databases like PostgreSQL to enable recovery after failure. In Delta Lake, the transaction log JSON files function as a WAL — the commit file is written atomically before data files are made visible to readers.

**YARN NodeManager**
In Hadoop YARN, the agent running on each worker node that manages containers, reports resource availability to the ResourceManager, and monitors container health. Spark executors run inside YARN containers managed by NodeManagers. The Spark ApplicationMaster also runs in a YARN container and negotiates executor containers from the ResourceManager.

**Zeppelin / Jupyter Notebook**
Interactive development environments used for data exploration and pipeline development. Apache Zeppelin is common in Hadoop-era platforms; Jupyter is common in Python/ML workflows. Databricks Notebooks are a managed equivalent with direct Spark integration, auto-versioning, and collaboration features built in.

---

**Parquet Footer**
The section at the end of a Parquet file that contains the file's schema, row group metadata, and column-level statistics (min, max, null count per row group). Query engines read the footer first (a small read) to determine whether any row group could satisfy the query predicate, then skip row groups that cannot — this is predicate pushdown / data skipping.

**Stage**
A unit of work in Spark that can be executed without a shuffle. A stage boundary is created wherever a wide transformation (shuffle) is required. Within a stage, all transformations are narrow (each task processes one partition independently). Stages within a job run sequentially; tasks within a stage run in parallel.

---

*This glossary covers terms from across the entire Big Data and Cloud Engineering curriculum — from Hadoop fundamentals to Kafka streaming, Azure cloud services, Delta Lake, and interview preparation. For Databricks-specific terminology, refer to the [Databricks Compendium Glossary](../databricks-doc/docs/glossary.md). For streaming-specific terms (watermark, trigger, checkpointing), see the [Streaming & Messaging module](09-streaming-messaging/index.md).*
