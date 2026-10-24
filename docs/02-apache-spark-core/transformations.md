# Spark Transformations & Functions

Most real pipeline code is a sequence of **column transformations** — select the right columns, derive new ones, filter rows, join against reference data, aggregate, and window. Spark ships a rich catalogue of built-in functions covering every category below. Prefer them over Python UDFs: built-ins run as JVM/Codegen code, UDFs round-trip through Python workers for every row.

Import conventions used throughout this page:

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, lit, when
from pyspark.sql.window import Window
```

## Column Selection

### `select`, `selectExpr`, `col()`, `expr()`, `alias()`

```python
# Four equivalent ways to reference a column
df.select("order_id",            # string name
          col("order_date"),      # Column object (supports chaining & operators)
          F.col("customer_id"),
          df["status"])

# Column expressions with alias
df.select(
    col("order_id").alias("id"),
    (col("price") * col("qty")).alias("line_total"),
    expr("UPPER(status) AS status_upper"),         # SQL fragment via expr()
)

# selectExpr: every argument is a SQL expression string
df.selectExpr(
    "order_id AS id",
    "price * qty  AS line_total",
    "UPPER(status) AS status_upper",
    "CASE WHEN qty > 10 THEN 'bulk' ELSE 'retail' END AS size_bucket",
)
```

| Function | Takes | Best for |
|----------|-------|----------|
| `select` | Column names or `Column` objects | Direct column references, dynamic column lists built in Python |
| `selectExpr` | SQL expression strings | Short SQL fragments; CASE expressions |
| `col("x")` | String | Everywhere; enables operator overloading (`col("x") + 1`) |
| `expr("...")` | SQL string | Dropping a full SQL fragment into any API point that takes a `Column` |

=== "PySpark"

    ```python
    adjusted = df.select(
        "order_item_id",
        when(col("product_name").like("%Nike%"),
             col("product_price") * 1.2)
          .otherwise(col("product_price"))
          .alias("adjusted_price"),
    )
    ```

=== "SQL"

    ```sql
    SELECT order_item_id,
           CASE WHEN product_name LIKE '%Nike%'
                THEN product_price * 1.2
                ELSE product_price END AS adjusted_price
    FROM   orders;
    ```

## Adding / Modifying / Removing Columns

### `withColumn`, `withColumnRenamed`, `drop`

```python
# Add or overwrite a column
df.withColumn("tax",           col("price") * 0.08)
df.withColumn("price",         col("price").cast("decimal(18,2)"))   # overwrite in place
df.withColumn("ingested_at",   F.current_timestamp())
df.withColumn("is_premium",    col("total") > 1000)

# Rename
df.withColumnRenamed("cust_id", "customer_id")

# Drop one or many
df.drop("tmp_col")
df.drop("tmp_col", "staging_hash", "load_batch")
```

!!! warning "Don't chain many withColumn() calls — build one select instead"
    Each `withColumn` creates a new logical plan node. Twenty chained `withColumn` calls build a 20-level plan tree that Catalyst must walk. For bulk column generation, prefer a single `select(*cols)` where `cols` is a list of `Column` expressions you built in Python. The result is a flat plan and often 2x faster to plan.

```python
# Pattern: one select, many derived columns
derived = [
    col("*"),
    (col("price") * col("qty")).alias("line_total"),
    F.when(col("qty") > 10, "bulk").otherwise("retail").alias("bucket"),
    F.current_timestamp().alias("ingested_at"),
]
df.select(*derived)
```

## Filtering

`filter` and `where` are aliases — identical behaviour, pick for readability.

=== "PySpark"

    ```python
    # SQL-string form
    df.filter("status = 'CLOSED' AND order_date >= '2024-01-01'")

    # Column-expression form
    df.filter((col("status") == "CLOSED") &
              (col("order_date") >= "2024-01-01"))

    # where() is just an alias for filter()
    df.where(col("qty") > 0)

    # IN / NOT IN
    df.filter(col("status").isin("CLOSED", "COMPLETE"))
    df.filter(~col("status").isin("CANCELED", "REFUNDED"))

    # Between (inclusive on both ends)
    df.filter(col("price").between(10, 100))

    # LIKE / RLIKE
    df.filter(col("product_name").like("%Nike%"))
    df.filter(col("product_name").rlike(r"^(Nike|Adidas)"))
    ```

=== "SQL"

    ```sql
    SELECT *
    FROM   orders
    WHERE  status = 'CLOSED'
       AND order_date >= '2024-01-01';
    ```

!!! tip "Chained filters are automatically fused and pushed down"
    Catalyst merges `filter(A).filter(B)` into `filter(A AND B)` and pushes the whole predicate into the Parquet/Delta reader. That means Parquet row groups outside the filter range are never read. Chaining filters is fine; worry about **where you filter** (as early as possible), not how many filter calls you make.

## Date & Timestamp Functions

Spark's default date pattern is `yyyy-MM-dd`. For anything else, pass an explicit format.

### Parsing

```python
# String → DateType / TimestampType
df.withColumn("order_date",  F.to_date  ("order_date_str",  "MM-dd-yyyy"))
df.withColumn("event_ts",    F.to_timestamp("event_ts_str", "yyyy-MM-dd HH:mm:ss"))

# Alternative: read as string, then cast — more robust under malformed inputs
raw = spark.read.schema("order_id long, order_date string, cust_id long, status string") \
                .csv("/data/orders/")
clean = raw.withColumn("order_date", F.to_date("order_date", "MM-dd-yyyy"))
```

!!! warning "MM vs mm"
    `MM` = month (01–12). `mm` = minutes (00–59). Mixing them up produces nonsense dates or silent nulls. Use uppercase for month, uppercase `HH` for 24-hour hour, lowercase `hh` for 12-hour.

### Formatting

```python
# Date → formatted string
df.withColumn("date_pretty", F.date_format("order_date", "dd MMM yyyy"))
df.withColumn("month_year",  F.date_format("order_date", "yyyyMM"))
```

### Arithmetic

```python
# Difference in days (date1 - date2)
df.withColumn("days_open",
              F.datediff(F.current_date(), col("order_date")))

# Add / subtract days
df.withColumn("ship_by",   F.date_add(col("order_date"),  7))
df.withColumn("ordered_7d_ago", F.date_sub(F.current_date(), 7))

# Months between (DOUBLE — fractional)
df.withColumn("months_old",
              F.months_between(F.current_date(), col("signup_date")))

# Add months — handles end-of-month correctly
df.withColumn("renewal", F.add_months(col("signup_date"), 12))
```

### Component extraction

```python
df.select(
    col("order_date"),
    F.year("order_date").alias("year"),
    F.month("order_date").alias("month"),
    F.dayofmonth("order_date").alias("day"),
    F.dayofweek("order_date").alias("dow"),
    F.weekofyear("order_date").alias("week"),
    F.quarter("order_date").alias("quarter"),
    F.hour("event_ts").alias("hour"),
    F.minute("event_ts").alias("minute"),
)
```

### Current date/time + truncation + windowed buckets

```python
df.withColumn("snapshot_ts", F.current_timestamp())
df.withColumn("snapshot_date", F.current_date())

# Truncate to the start of the month / year / hour
df.withColumn("month_start", F.date_trunc("month", "event_ts"))
df.withColumn("hour_start",  F.date_trunc("hour",  "event_ts"))

# Last day of the month
df.withColumn("month_end", F.last_day("order_date"))
```

## String Functions

```python
# Concatenation
df.select(F.concat   (col("first_name"), F.lit(" "), col("last_name")).alias("full"))
df.select(F.concat_ws("-", col("country"), col("state"), col("city")).alias("region"))

# Case
df.select(F.upper("name"), F.lower("name"), F.initcap("name"))

# Trim
df.select(F.trim("name"), F.ltrim("name"), F.rtrim("name"))

# Length
df.select(F.length("sku"))                 # character length
df.select(F.octet_length("sku"))           # byte length (matters for UTF-8)

# Substring: substring(col, pos, len) — 1-indexed, inclusive
df.select(F.substring("sku", 1, 3).alias("brand_prefix"))
df.select(F.substring_index("email", "@", 1).alias("local_part"))

# Split → array
df.select(F.split("tags", ","))

# Position of substring (1-based; 0 if not found)
df.select(F.instr("email", "@").alias("at_pos"))

# Replace (literal and regex)
df.select(F.regexp_replace("phone", r"[^0-9]", ""))

# Extract first match
df.select(F.regexp_extract("ua_string", r"Firefox/([\d.]+)", 1)
          .alias("firefox_version"))

# Left-pad / right-pad
df.select(F.lpad("code", 10, "0"))         # "0000000123"
df.select(F.rpad("code", 10, " "))

# Translate — char-by-char remap
df.select(F.translate("ssn", "0123456789", "**********"))
```

=== "PySpark"

    ```python
    cleaned = (df
      .withColumn("email_domain",
                  F.regexp_extract(col("email"), r"@(.+)$", 1))
      .withColumn("phone_e164",
                  F.concat(F.lit("+"), F.regexp_replace(col("phone"), r"\D", ""))))
    ```

=== "SQL"

    ```sql
    SELECT *,
           regexp_extract(email, '@(.+)$', 1)                 AS email_domain,
           concat('+', regexp_replace(phone, '\\D', ''))      AS phone_e164
    FROM   customers;
    ```

## Type Casting

```python
# cast — accepts a type string or DataType object
df.withColumn("amount", col("amount_str").cast("decimal(18,2)"))
df.withColumn("qty",    col("qty_str").cast("int"))
df.withColumn("event_ts", col("event_ts_str").cast("timestamp"))

# try_cast (Spark 3.x) — returns NULL on cast failure instead of throwing
df.withColumn("qty_safe", F.expr("try_cast(qty_str AS int)"))
```

!!! tip "try_cast for messy upstream data"
    `cast` throws when the value doesn't fit (e.g. `"NaN"` → int). `try_cast` returns `NULL`. In an ingestion job, always use `try_cast` and quarantine rows where the result is `NULL` — that's your malformed-data report.

## Null Handling

```python
# Predicate helpers
df.filter(col("email").isNull())
df.filter(col("email").isNotNull())

# Drop rows with nulls
df.dropna()                                   # any null in any column
df.dropna(how="all")                          # only drop fully-null rows
df.dropna(subset=["email", "phone"])          # null in any of these
df.dropna(thresh=3)                           # keep rows with at least 3 non-nulls

# Fill nulls
df.fillna(0)                                   # fill all numeric columns
df.fillna("UNKNOWN", subset=["status"])
df.fillna({"status": "UNKNOWN",
           "qty":    0,
           "price":  0.0})

# Return the first non-null value (like SQL COALESCE)
df.withColumn("contact",
              F.coalesce(col("mobile"), col("home"), col("work"), F.lit("n/a")))

# nvl / nvl2 / ifnull — SQL-style aliases
df.selectExpr("nvl(discount, 0) AS discount",
              "ifnull(coupon, 'NONE') AS coupon",
              "nvl2(premium_date, 'premium', 'standard') AS tier")
```

## Deduplication: `distinct()` vs `dropDuplicates(subset)`

```python
# distinct — considers EVERY column
unique_rows = df.distinct()

# dropDuplicates — considers only the subset you pass
unique_customers = df.dropDuplicates(["customer_id"])
latest_per_customer = (
    df.orderBy(col("event_ts").desc())
      .dropDuplicates(["customer_id"])         # keeps the first row per customer
)
```

| Method | Deduplication key |
|--------|-------------------|
| `distinct()` | All columns in the row |
| `dropDuplicates()` | All columns (same as `distinct`) |
| `dropDuplicates(subset=[...])` | Only the named columns |

!!! info "Performance: identical for 'all columns', different for subsets"
    `distinct()` and `dropDuplicates()` with no subset produce the same physical plan (hash aggregate on the whole row). `dropDuplicates(subset)` hashes only the subset columns — cheaper when you need "latest per key" style logic. For picking a specific row per group, prefer **window functions** (`row_number().over(...)`) — they give you deterministic ordering, which `dropDuplicates` alone does not guarantee across runs.

## Joins

Spark supports every SQL join type. The two decisive design choices are **join type** (how unmatched rows are handled) and **join strategy** (how Spark physically executes the join — broadcast, sort-merge, shuffle-hash).

### Join types

| Type | PySpark `how=` | Keeps left-only rows? | Keeps right-only rows? |
|------|---------------|----------------------|------------------------|
| **Inner** | `"inner"` *(default)* | No | No |
| **Left outer** | `"left"` / `"left_outer"` | Yes (right columns null) | No |
| **Right outer** | `"right"` / `"right_outer"` | No | Yes (left columns null) |
| **Full outer** | `"full"` / `"outer"` / `"full_outer"` | Yes | Yes |
| **Left semi** | `"left_semi"` | Only left rows with a right match (right columns dropped) | No |
| **Left anti** | `"left_anti"` | Only left rows with **no** right match | No |
| **Cross** | `"cross"` | Every left × every right (cartesian) | — |

### Join syntax

=== "PySpark"

    ```python
    # Equi-join on a single column — simplest form
    orders_customers = orders.join(customers, "customer_id", "inner")

    # Multiple keys
    inv_orders = orders.join(items, ["order_id", "line_num"], "left")

    # Different column names on each side
    orders_customers = orders.join(
        customers,
        orders["cust_id"] == customers["id"],
        "left"
    )

    # Complex predicate
    txn_with_fx = txns.join(
        fx_rates,
        (txns["currency"] == fx_rates["currency"]) &
        (txns["txn_date"].between(fx_rates["valid_from"], fx_rates["valid_to"])),
        "left"
    )

    # Semi join: "orders where the customer exists in customers"
    valid = orders.join(customers, "customer_id", "left_semi")

    # Anti join: "orders whose customer is missing from customers"
    orphans = orders.join(customers, "customer_id", "left_anti")

    # Explicit cross join — fails without this if spark.sql.crossJoin.enabled=false
    grid = products.crossJoin(stores)
    ```

=== "SQL"

    ```sql
    SELECT o.*, c.name
    FROM   orders o
    LEFT   JOIN customers c
      ON   o.customer_id = c.id;

    -- Semi
    SELECT *
    FROM   orders o
    LEFT   SEMI JOIN customers c
      ON   o.customer_id = c.id;

    -- Anti
    SELECT *
    FROM   orders o
    LEFT   ANTI JOIN customers c
      ON   o.customer_id = c.id;
    ```

### When to use which join

- **Inner** — "give me rows that exist on both sides". The default for lookups where unmatched is a data error.
- **Left outer** — "keep every row from my fact table, enrich if a dim match exists". The workhorse of Silver/Gold enrichment.
- **Full outer** — reconciliation / diff workloads where both sides matter.
- **Left semi** — faster, cleaner replacement for `WHERE id IN (SELECT id FROM other)`. Returns no right-side columns.
- **Left anti** — "find orphans" — rows in A that are missing from B. Perfect for data quality.
- **Cross** — grids, parameter sweeps, feature expansion. Rare; always sanity-check row counts first.

!!! danger "The cross-join explosion"
    A classic bug: forgetting a join condition. `A.join(B)` with no condition **silently becomes a cross join** on older Spark versions. Set `spark.sql.crossJoin.enabled=false` (default from Spark 3.0) so Spark raises an error unless you explicitly ask for a cross join. Always verify the pre-join row count against the post-join count.

### Broadcast hints — the single biggest join optimisation

If one side is small (a few hundred MB), broadcasting it eliminates the shuffle entirely:

```python
from pyspark.sql.functions import broadcast

enriched = orders.join(broadcast(countries), "country_code", "left")
```

Spark auto-broadcasts tables under `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), but statistics must be accurate; use the hint when you know a side is small and the stats might be wrong.

## Aggregations

### `groupBy` + `agg`

```python
# Single aggregation
daily = orders.groupBy("order_date").count()
daily = orders.groupBy("order_date").sum("amount")

# Multiple aggregations with aliases (preferred)
summary = (orders
    .groupBy("order_date", "store_id")
    .agg(
        F.count("*").alias("n_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_order_value"),
        F.min("amount").alias("min_order"),
        F.max("amount").alias("max_order"),
        F.stddev("amount").alias("stddev_order"),
    ))
```

### Full aggregate function catalogue

| Category | Functions |
|----------|-----------|
| **Counting** | `count`, `count("*")`, `countDistinct`, `approx_count_distinct` (HyperLogLog, 2% error) |
| **Sums** | `sum`, `sumDistinct` |
| **Averages** | `avg` / `mean`, `median` (3.4+) |
| **Extremes** | `min`, `max`, `first`, `last` (with `ignorenulls`) |
| **Dispersion** | `stddev`, `stddev_pop`, `variance`, `var_pop` |
| **Collection** | `collect_list` (keeps duplicates + order), `collect_set` (deduped, unordered) |
| **Logical** | `bool_and`, `bool_or`, `any_value` |
| **Percentile** | `percentile_approx("x", 0.5)` (approximate), `percentile("x", 0.5)` (exact; expensive) |

```python
# collect_list / collect_set — array-valued aggregates
per_customer = (orders
    .groupBy("customer_id")
    .agg(
        F.collect_list("order_id").alias("all_orders"),
        F.collect_set("status").alias("distinct_statuses"),
    ))
```

### Rollups and cubes

```python
# Hierarchical totals: (store, date), (store, null), (null, null)
orders.rollup("store_id", "order_date").agg(F.sum("amount"))

# Every combination: (store, date), (store, null), (null, date), (null, null)
orders.cube("store_id", "order_date").agg(F.sum("amount"))

# Grouping sets via SQL
spark.sql("""
    SELECT store_id, order_date, SUM(amount) AS rev
    FROM   orders
    GROUP  BY GROUPING SETS ((store_id), (order_date), (store_id, order_date))
""")
```

### Pivot

```python
# Rows × columns matrix: orders per customer per status
pivoted = (orders
    .groupBy("customer_id")
    .pivot("status", ["CLOSED", "PENDING_PAYMENT", "CANCELED"])  # explicit value list is faster
    .count())
```

!!! tip "Always pass the explicit value list to pivot()"
    Without the list, Spark runs an extra job to discover the distinct values. Passing them saves a full data scan.

## Window Functions

Window functions compute a value **per row** using a frame of surrounding rows — ranking, running totals, lag/lead, moving averages. They do not reduce row count (unlike `groupBy`).

### Defining a window

```python
from pyspark.sql.window import Window

# Partition by customer, order by event time
w = Window.partitionBy("customer_id").orderBy("event_ts")

# With a frame — e.g. 6-row trailing average (current + 5 preceding)
w_trailing = (Window
    .partitionBy("customer_id")
    .orderBy("event_ts")
    .rowsBetween(-5, 0))

# Range frame — bounded by value, not row count (useful for time-based windows)
w_days = (Window
    .partitionBy("customer_id")
    .orderBy(F.col("event_ts").cast("long"))
    .rangeBetween(-7 * 86400, 0))        # last 7 days
```

### Common window functions

```python
scored = df.withColumn(
    "running_total", F.sum("amount").over(w)
).withColumn(
    "trailing_6_avg", F.avg("amount").over(w_trailing)
).withColumn(
    # Rank: 1,2,2,4 (ties share, gaps after)
    "rnk", F.rank().over(w)
).withColumn(
    # Dense rank: 1,2,2,3 (ties share, no gaps)
    "d_rnk", F.dense_rank().over(w)
).withColumn(
    # Row number: 1,2,3,4 (unique, deterministic given ordering)
    "rn", F.row_number().over(w)
).withColumn(
    # Previous row's amount (NULL for first row in partition)
    "prev_amount", F.lag("amount", 1).over(w)
).withColumn(
    # Next row's amount
    "next_amount", F.lead("amount", 1).over(w)
).withColumn(
    # Percent rank (0.0 → 1.0 within partition)
    "pct", F.percent_rank().over(w)
).withColumn(
    # Divide partition into 4 buckets
    "quartile", F.ntile(4).over(w)
)
```

### Classic pattern: latest row per key

=== "PySpark"

    ```python
    latest = (df
      .withColumn("rn",
                  F.row_number().over(Window
                      .partitionBy("customer_id")
                      .orderBy(F.col("event_ts").desc())))
      .filter("rn = 1")
      .drop("rn"))
    ```

=== "SQL"

    ```sql
    SELECT *
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY customer_id
                                ORDER BY event_ts DESC) AS rn
      FROM   events
    )
    WHERE rn = 1;
    ```

### Running total / moving average pattern

=== "PySpark"

    ```python
    w7 = (Window.partitionBy("customer_id")
                .orderBy("event_ts")
                .rowsBetween(-6, 0))

    with_metrics = (df
      .withColumn("cum_amount",  F.sum("amount").over(
          Window.partitionBy("customer_id").orderBy("event_ts")))
      .withColumn("avg_7d",       F.avg("amount").over(w7)))
    ```

=== "SQL"

    ```sql
    SELECT *,
           SUM(amount) OVER (PARTITION BY customer_id
                             ORDER BY event_ts)                               AS cum_amount,
           AVG(amount) OVER (PARTITION BY customer_id
                             ORDER BY event_ts
                             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)        AS avg_7d
    FROM   events;
    ```

!!! warning "Order by in a window matters — and so does the frame"
    `rank()` without `orderBy` is nonsense. `sum().over(partitionBy(...))` with no `orderBy` sums the entire partition for every row (OK if that's what you want). With `orderBy` but no explicit frame, the default is `rangeBetween(unboundedPreceding, currentRow)` for aggregates — i.e. a running total. Always check the frame semantics explicitly for anything beyond simple ranking.

## User-Defined Functions (UDFs)

When a built-in truly cannot express your logic, you can register a Python function as a UDF. **This is usually the wrong answer** — read the warning first.

### Python UDF

```python
from pyspark.sql.types import StringType, DoubleType

# 1. Decorator style
@F.udf(returnType=StringType())
def grade(score):
    if score >= 90: return "A"
    if score >= 80: return "B"
    if score >= 70: return "C"
    return "F"

df.withColumn("grade", grade(col("score")))

# 2. Function-call style with explicit registration
def tax_calc(amount, rate):
    if amount is None or rate is None:
        return None
    return float(amount) * float(rate)

tax_udf = F.udf(tax_calc, DoubleType())
df.withColumn("tax", tax_udf(col("amount"), col("tax_rate")))

# 3. Register for SQL
spark.udf.register("grade_sql", grade.func, StringType())
spark.sql("SELECT *, grade_sql(score) AS grade FROM scores")
```

### Pandas UDF (vectorised) — much faster when you must use Python

Pandas UDFs batch rows into a `pandas.Series` and apply the function once per batch, cutting the per-row Python call overhead by orders of magnitude.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def tax_pandas(amount: pd.Series, rate: pd.Series) -> pd.Series:
    return amount * rate

df.withColumn("tax", tax_pandas(col("amount"), col("tax_rate")))

# Grouped-map pandas UDF — pandas DataFrame in, DataFrame out
@pandas_udf("customer_id long, zscore double")
def z_by_customer(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["zscore"] = (pdf["amount"] - pdf["amount"].mean()) / pdf["amount"].std(ddof=0)
    return pdf[["customer_id", "zscore"]]

df.groupBy("customer_id").applyInPandas(z_by_customer, schema="customer_id long, zscore double")
```

!!! warning "UDF performance penalty"
    A Python UDF forces Spark to:

    1. Serialise each row from the JVM to a Python worker (pickle).
    2. Call your Python function.
    3. Serialise the result back to the JVM.
    4. Repeat for every single row.

    Catalyst **cannot optimise through a UDF** — it is an opaque black box. Predicate pushdown, column pruning, codegen fusion all stop at the UDF boundary.

    **Prefer** in order:

    1. A built-in function (`F.*`) — stays in JVM, fully optimisable.
    2. A SQL expression via `expr()` — same benefit.
    3. A pandas UDF — vectorised, 10x–100x faster than a plain Python UDF.
    4. A plain Python UDF — only as last resort.

### Rewriting a UDF as built-ins — the typical speedup

```python
# SLOW: Python UDF
@F.udf("string")
def email_domain(email):
    return email.split("@")[1] if email and "@" in email else None

df.withColumn("domain", email_domain(col("email")))

# FAST: equivalent built-in
df.withColumn("domain", F.regexp_extract(col("email"), r"@(.+)$", 1))
```

On a 1-billion-row dataset, the built-in version typically runs **5–20x faster** and uses a fraction of the executor memory.

## Putting It Together — A Realistic Pipeline Snippet

=== "PySpark"

    ```python
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # 1. Read raw with explicit schema + permissive mode
    orders = (spark.read
        .schema("order_id long, order_date string, customer_id long, "
                "status string, amount decimal(18,2)")
        .option("mode", "permissive")
        .csv("/data/bronze/orders/*.csv"))

    # 2. Clean + enrich
    cleaned = (orders
        .filter("status IS NOT NULL AND amount > 0")
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["order_id"]))

    # 3. Join against customer dim (small — broadcast)
    customers = spark.table("silver.customers").select("customer_id", "country")
    enriched  = cleaned.join(F.broadcast(customers), "customer_id", "left")

    # 4. Window: order rank per customer
    rank_w = Window.partitionBy("customer_id").orderBy("order_date")
    ranked = enriched.withColumn("order_seq", F.row_number().over(rank_w))

    # 5. Aggregate: daily revenue per country
    daily_country = (ranked
        .groupBy("order_date", "country")
        .agg(
            F.sum("amount").alias("revenue"),
            F.countDistinct("customer_id").alias("active_customers"),
            F.count("*").alias("orders"),
        ))

    # 6. Write to Gold
    (daily_country.write
         .format("delta").mode("overwrite")
         .partitionBy("order_date")
         .saveAsTable("gold.daily_country_revenue"))
    ```

=== "SQL"

    ```sql
    CREATE OR REPLACE TABLE gold.daily_country_revenue
    USING delta
    PARTITIONED BY (order_date)
    AS
    WITH cleaned AS (
      SELECT order_id,
             to_date(order_date, 'yyyy-MM-dd')      AS order_date,
             customer_id,
             status,
             amount,
             current_timestamp()                    AS ingested_at
      FROM   bronze.orders
      WHERE  status IS NOT NULL
        AND  amount > 0
      QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date) = 1
    ),
    enriched AS (
      SELECT o.*, c.country
      FROM   cleaned o
      LEFT   JOIN silver.customers c
             ON o.customer_id = c.customer_id
    )
    SELECT order_date,
           country,
           SUM(amount)                    AS revenue,
           COUNT(DISTINCT customer_id)    AS active_customers,
           COUNT(*)                       AS orders
    FROM   enriched
    GROUP  BY order_date, country;
    ```

## Where to Go Next

- **[DataFrames & SparkSQL](dataframes.md)** — the full read/write/catalog story these transformations plug into.
- **[RDDs](rdds.md)** — the low-level API, for the rare cases where DataFrames aren't enough.
- **[Apache Spark Core](index.md)** — the execution model, DAG, and cluster architecture underpinning all of the above.
