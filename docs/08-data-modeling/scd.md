# Slowly Changing Dimensions (SCD)

Dimension attributes are not static. Customers move cities. Products change categories. Sales representatives transfer to new territories. Slowly Changing Dimensions (SCD) define the strategies for handling these changes in a dimensional model — specifically, whether to preserve historical values, overwrite them, or track multiple versions simultaneously.

Choosing the wrong SCD strategy is a common source of analytical errors. A report that groups customers by their *current* city instead of their *city at the time of purchase* will misattribute historical revenue. Understanding each SCD type, its trade-offs, and when to apply it is a core data engineering competency.

## SCD Type 0 — Fixed (Never Change)

Some attributes represent facts that were true at a moment in time and must never be overwritten, regardless of what the source system reports later. These attributes are written once on the initial load and thereafter ignored by the SCD pipeline.

**Examples:** `date_of_birth`, `place_of_birth`, `original_credit_score`, `account_open_date`

**Implementation:** No special logic required. The ETL pipeline simply does not update these columns after the initial insert.

| customer_id | date_of_birth | original_credit_score |
|-------------|--------------|----------------------|
| 101         | 1985-04-12   | 720                  |

If the source system sends a corrected `original_credit_score`, the pipeline ignores it. The original value is preserved as the immutable baseline.

## SCD Type 1 — Overwrite (No History)

The incoming value overwrites the existing value. No history is retained. The dimension row always reflects the current state.

**Use when:** the change is a correction (fixing a data entry error, correcting a misspelled name) or when the historical value has no analytical value.

**Examples:** correcting a typo in a customer name, updating a non-critical contact email.

### Before Update

| customer_skey | customer_id | customer_name | city      |
|--------------|------------|---------------|-----------|
| 1001          | 101        | Jon Doe       | Bangalore |

### After SCD Type-1 Update

```sql
UPDATE gold.shared.DimCustomer
SET    customer_name = 'John Doe'
WHERE  customer_id   = 101;
```

| customer_skey | customer_id | customer_name | city      |
|--------------|------------|---------------|-----------|
| 1001          | 101        | John Doe      | Bangalore |

!!! warning "Type-1 breaks historical reports"
    Any query that ran before the update and used `customer_name` will now return different results if re-run after the update. This is expected and acceptable for Type-1 — the old value was wrong. Never use Type-1 for attributes where historical accuracy matters.

## SCD Type 2 — Add New Row (Full History)

A new row is inserted for every change to a tracked attribute. The previous row is *expired* — its end date is set and its active flag is cleared. The new row receives a new surrogate key, today as its effective date, a far-future sentinel expiry date (`9999-12-31`), and an active flag of `TRUE`.

SCD Type-2 is the industry standard for dimensions where historical accuracy is required. It is the correct strategy for customer addresses, product pricing, sales territory assignments, and any attribute that affects historical analytics.

### Key Columns for SCD Type-2

| Column | Purpose |
|--------|---------|
| `customer_skey` | Surrogate key — new value for each version |
| `customer_id` | Natural key — same across all versions of a customer |
| `effective_date` | Date from which this version is valid |
| `expiry_date` | Date on which this version expires (`9999-12-31` = still active) |
| `is_current` | Boolean — `TRUE` for the single active version |
| `hash_md5` | MD5 hash of tracked attributes — used for change detection |

### State Before and After a City Change

**Initial state — customer 101 lives in Bangalore:**

| customer_id | city      | customer_skey | effective_date | expiry_date | is_current |
|------------|-----------|--------------|----------------|-------------|------------|
| 101        | Bangalore | 1001          | 2026-01-01     | 9999-12-31  | TRUE       |

**Customer moves to Delhi. SCD Type-2 process:**

Step 1 — expire the old record:

| customer_id | city      | customer_skey | effective_date | expiry_date | is_current |
|------------|-----------|--------------|----------------|-------------|------------|
| 101        | Bangalore | 1001          | 2026-01-01     | 2026-07-26  | FALSE      |

Step 2 — insert new version:

| customer_id | city  | customer_skey | effective_date | expiry_date | is_current |
|------------|-------|--------------|----------------|-------------|------------|
| 101        | Delhi | 1002          | 2026-07-26     | 9999-12-31  | TRUE       |

**Historical orders placed before July 26 will still join to `customer_skey=1001` (Bangalore). Orders placed after will join to `customer_skey=1002` (Delhi).** This is the fundamental value of SCD Type-2.

## SCD Type 3 — Add Column (Partial History)

An additional column is added to the existing dimension row to capture the previous value alongside the current value. Only the immediate prior state is preserved — history deeper than one change is lost.

**Use when:** only the most recent transition matters and full history is not required. Typical for role or territory changes where analysts only need to compare current vs. previous.

```sql
ALTER TABLE gold.shared.DimEmployee
ADD COLUMN previous_territory VARCHAR(100);
```

| employee_id | current_territory | previous_territory |
|------------|------------------|-------------------|
| 501        | West              | East               |

!!! info "Type-3 is rarely the right choice"
    The limitation to one previous value makes Type-3 fragile. A second territory change overwrites `previous_territory`, losing the first value permanently. Type-2 covers all cases Type-3 covers, with full history. Reserve Type-3 only when the dimension is guaranteed never to change more than once per record, or when the schema is too rigid to add new rows.

## SCD Type 4 — History Table (Split Architecture)

Type-4 maintains two separate tables: a **current** table containing only the latest version of each record, and a **history** table containing all previous versions. The current table is optimized for lookups; the history table is optimized for audit and time-series analysis.

```sql
-- Current table: one row per entity, always current state
CREATE TABLE gold.shared.DimCustomerCurrent (
    customer_id    INT PRIMARY KEY,
    customer_name  VARCHAR(200),
    city           VARCHAR(100),
    updated_date   DATE
);

-- History table: full version history
CREATE TABLE gold.shared.DimCustomerHistory (
    customer_id     INT,
    customer_name   VARCHAR(200),
    city            VARCHAR(100),
    effective_date  DATE,
    expiry_date     DATE
);
```

Type-4 is less common than Type-2 because it splits the dimension across two tables, complicating join logic. It is useful when the current and historical use cases have very different performance or access control requirements.

## SCD Type 6 — Hybrid (1 + 2 + 3)

Type-6 combines Type-1, Type-2, and Type-3 in a single dimension row. Each version is tracked as a new row (Type-2), but the row also carries the current value of the attribute as an additional column (Type-1 overwrite on all rows), and a previous-value column (Type-3). The name derives from 1 × 2 × 3 = 6.

| customer_id | city_at_version | current_city | previous_city | customer_skey | effective_date | expiry_date | is_current |
|------------|----------------|-------------|--------------|--------------|----------------|-------------|------------|
| 101        | Bangalore      | Delhi        | Bangalore     | 1001          | 2026-01-01     | 2026-07-26  | FALSE      |
| 101        | Delhi          | Delhi        | Bangalore     | 1002          | 2026-07-26     | 9999-12-31  | TRUE       |

`city_at_version` is the historically accurate value for that row (Type-2). `current_city` is overwritten on all rows when a change occurs (Type-1). `previous_city` carries the most recent prior value (Type-3).

This lets queries use `city_at_version` for historical accuracy and `current_city` for current-state queries — without joining back to a separate current table.

## SCD Type Decision Table

| Criteria | Type 0 | Type 1 | Type 2 | Type 3 | Type 4 | Type 6 |
|----------|--------|--------|--------|--------|--------|--------|
| **Historical accuracy required** | N/A (immutable) | No | Yes | Partial | Yes | Yes |
| **Full version history** | N/A | No | Yes | No | Yes | Yes |
| **Storage overhead** | None | None | Medium | None | Medium | High |
| **Query complexity** | Low | Low | Medium | Low | High | Medium |
| **ETL complexity** | Lowest | Low | High | Low | Medium | Highest |
| **Use when** | Attribute never changes | Correction / low-value change | Address, price, territory — any analytically important change | Only need current vs. previous | Separate current/history access patterns | Need both historical accuracy and current-state columns |

---

## Full PySpark SCD Type-2 Implementation

The standard PySpark SCD Type-2 pattern uses MD5 hashing to detect attribute changes, a full outer join to classify each record's required action, and a union of all result streams written back to the dimension table.

### Pipeline Overview

```
Source Batch (new data)          Target Dimension (active rows only)
       |                                        |
  Generate MD5 hash                   Filter is_current = TRUE
  on tracked columns                  Generate MD5 hash
       |                                        |
       +----------------+  +-------------------+
                        |  |
               FULL OUTER JOIN on natural key
                        |
           Classify: INSERT / UPDATE / DELETE / NO_CHANGE
                        |
        +---------------+---------------+
        |               |               |
     INSERT          UPDATE          NO_CHANGE
    (new rows)   (expire old +      (retain as-is)
                  insert new)
                        |
              UNION all result streams
                        |
             Write back to dimension table
```

### Complete Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, md5, concat_ws, lit, current_date,
    when, row_number, coalesce
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SCD_Type_2_Pipeline").getOrCreate()

# =============================================================================
# STEP 1: DEFINE SCHEMAS AND LOAD SOURCE / TARGET DATASETS
# =============================================================================

source_path = "/data/scd_demo/source"
target_path = "/data/scd_demo/target"

source_schema = """
    customerid LONG, firstname STRING, lastname STRING, email STRING,
    phone STRING, address STRING, city STRING, state STRING, zipcode LONG
"""

target_schema = """
    customerid LONG, firstname STRING, lastname STRING, email STRING,
    phone STRING, address STRING, city STRING, state STRING, zipcode LONG,
    customer_skey LONG, effective_date DATE, end_date DATE, is_current BOOLEAN
"""

# Read the incoming operational source batch
source_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(source_schema)
    .load(source_path)
)

# Read the existing Data Warehouse target dimension
target_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(target_schema)
    .load(target_path)
)

# =============================================================================
# STEP 2: GENERATE MD5 HASHES FOR CHANGE DETECTION
# =============================================================================

# Only these attributes trigger a new SCD Type-2 version when they change
slowly_changing_cols = [
    "firstname", "lastname", "email",
    "phone", "address", "city", "state", "zipcode"
]


def add_hash(df, cols, suffix):
    """
    Computes an MD5 hash over the specified columns and renames all
    columns with the given suffix to avoid ambiguity after the join.
    Uses COALESCE to handle NULLs — a NULL in any column would otherwise
    cause the entire hash to be NULL, producing false change detections.
    """
    hash_expr = md5(
        concat_ws(
            "||",
            *[coalesce(col(c).cast("string"), lit("")) for c in cols]
        )
    )

    df_hashed = df.withColumn(f"hash_md5_{suffix}", hash_expr)
    for c in df.columns:
        df_hashed = df_hashed.withColumnRenamed(c, f"{c}_{suffix}")
    return df_hashed


# Isolate only the currently active target records for comparison
active_target_df = target_df.filter(col("is_current") == True)

src_hashed = add_hash(source_df, slowly_changing_cols, "source")

tgt_hashed = add_hash(
    active_target_df,
    slowly_changing_cols + ["customer_skey", "effective_date", "end_date", "is_current"],
    "target"
)

# =============================================================================
# STEP 3: FULL OUTER JOIN FOR DELTA IDENTIFICATION
# =============================================================================

joined_df = src_hashed.join(
    tgt_hashed,
    col("customerid_source") == col("customerid_target"),
    "full_outer"
)

# =============================================================================
# STEP 4: CLASSIFY EACH RECORD'S REQUIRED ACTION
# =============================================================================

delta_df = joined_df.withColumn(
    "action_flag",
    when(col("customerid_target").isNull(),                            lit("INSERT"))
    .when(col("customerid_source").isNull(),                           lit("DELETE"))
    .when(col("hash_md5_source") != col("hash_md5_target"),            lit("UPDATE"))
    .otherwise(                                                         lit("NO_CHANGE"))
)

# =============================================================================
# STEP 5: PRODUCE THE FOUR OUTPUT STREAMS
# =============================================================================

# Stream A: unchanged records — pass through as-is
no_change_records = (
    target_df.join(
        delta_df.filter(col("action_flag") == "NO_CHANGE"),
        target_df.customer_skey == delta_df.customer_skey_target
    )
    .select(target_df["*"])
)

# Stream B: deleted records — close the active row
deleted_records = (
    target_df.join(
        delta_df.filter(col("action_flag") == "DELETE"),
        target_df.customer_skey == delta_df.customer_skey_target
    )
    .select(target_df["*"])
    .withColumn("end_date", current_date())
    .withColumn("is_current", lit(False))
)

# Stream C: old versions of updated records — expire them
expired_history_records = (
    target_df.join(
        delta_df.filter(col("action_flag") == "UPDATE"),
        target_df.customer_skey == delta_df.customer_skey_target
    )
    .select(target_df["*"])
    .withColumn("end_date", current_date())
    .withColumn("is_current", lit(False))
)

# Stream D: new rows for INSERT and UPDATE actions
new_inserts = (
    delta_df.filter(col("action_flag").isin("INSERT", "UPDATE"))
    .select(
        col("customerid_source").alias("customerid"),
        col("firstname_source").alias("firstname"),
        col("lastname_source").alias("lastname"),
        col("email_source").alias("email"),
        col("phone_source").alias("phone"),
        col("address_source").alias("address"),
        col("city_source").alias("city"),
        col("state_source").alias("state"),
        col("zipcode_source").alias("zipcode"),
    )
)

# Assign new surrogate keys (max existing skey + sequential offset)
max_skey = target_df.agg({"customer_skey": "max"}).collect()[0][0] or 0
window_spec = Window.orderBy("customerid")

formatted_new_inserts = (
    new_inserts
    .withColumn("customer_skey", max_skey + row_number().over(window_spec))
    .withColumn("effective_date", current_date())
    .withColumn("end_date", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)

# =============================================================================
# STEP 6: UNION ALL STREAMS AND WRITE BACK TO DIMENSION TABLE
# =============================================================================

# Carry forward already-inactive historical records from previous runs
inactive_target_history = target_df.filter(col("is_current") == False)

final_dimension_df = (
    inactive_target_history
    .union(no_change_records)
    .union(deleted_records)
    .union(expired_history_records)
    .union(formatted_new_inserts)
)

final_dimension_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save(target_path)
```

### Action Classification Reference

| Source Record | Target Active Record | Hashes | Action |
|--------------|---------------------|--------|--------|
| Exists | Does not exist | — | **INSERT** — new customer, create first version |
| Does not exist | Exists | — | **DELETE** — customer removed, expire active row |
| Exists | Exists | Differ | **UPDATE** — expire old version, insert new version |
| Exists | Exists | Match | **NO_CHANGE** — retain active record unchanged |

!!! warning "Window function ordering and surrogate key assignment"
    `row_number().over(Window.orderBy("customerid"))` assigns sequential integers by `customerid`. In a distributed Spark job this ordering is deterministic within a single run but is not globally stable across runs. For production pipelines, prefer a dedicated surrogate key sequence (e.g., using a Delta table as a counter, or a UUID-based key) to avoid potential collisions if the pipeline is restarted mid-run.

!!! tip "Using Delta Lake MERGE instead of overwrite"
    For very large dimension tables, replacing the full `overwrite` write with a Delta `MERGE` operation is more efficient. Use `MERGE` to update expired rows in place and insert new rows, rather than rewriting the entire dimension table on every pipeline run. See the [Delta Lake page](../05-delta-lake/index.md) for the MERGE pattern.

### SCD Type-2 Key Column Reference

| Column | Data Type | Purpose |
|--------|-----------|---------|
| `customer_id` | INT / LONG | Natural key — identifies the business entity; repeats across versions |
| `customer_skey` | BIGINT | Surrogate key — uniquely identifies each version; referenced by fact tables |
| `effective_date` | DATE | First date this version is valid |
| `end_date` | DATE | Last date this version is valid; `9999-12-31` means currently active |
| `is_current` | BOOLEAN | `TRUE` for exactly one row per natural key at any point in time |
| `hash_md5` | VARCHAR(32) | MD5 of tracked slow-change attributes; drives change detection |

!!! info "Why 9999-12-31 as the sentinel date?"
    Using a far-future date as the end date for active records simplifies range queries. To find the version active on any given date: `WHERE effective_date <= :query_date AND end_date >= :query_date`. Using NULL for open-ended records requires IS NULL checks and often breaks join optimizers. The sentinel value is universally supported by SQL date types and Parquet.
