# Normalization (1NF, 2NF, 3NF)

Normalization is the systematic process of restructuring a relational database schema to eliminate data redundancy and prevent data anomalies. Every normal form adds a new structural constraint on top of the previous one, progressively tightening the rules about how columns depend on keys.

## Why Normalize? The Three Anomalies

Before a schema is normalized, it is vulnerable to three categories of corruption that occur as data evolves:

**Insert anomaly** — You cannot record a fact without also recording an unrelated fact. Example: you cannot add a new course to a course catalog unless at least one student is enrolled, because student and course data live in the same table.

**Update anomaly** — Changing one fact requires updating multiple rows. Example: if a supplier's address is stored on every product row, changing the address means touching thousands of rows. Any partial failure leaves the database inconsistent.

**Delete anomaly** — Deleting one fact inadvertently destroys another. Example: dropping the last student enrolled in a course also erases the course's own details from the database.

Normalization solves all three anomalies by ensuring each fact is stored in exactly one place.

!!! info "Normalization is for OLTP"
    Normalization is the correct design strategy for transactional systems where writes are frequent and data integrity is non-negotiable. For analytical workloads, the trade-off reverses — see [Denormalization](#denormalization) below.

## Functional Dependencies

Before defining the normal forms, the concept of **functional dependency** is essential.

A functional dependency `A → B` means: for every value of `A`, there is exactly one corresponding value of `B`. In other words, knowing `A` determines `B`.

Examples:

- `student_id → student_name` — each student ID maps to exactly one name
- `zip_code → city` — a zip code determines the city (within a country)
- `(order_id, product_id) → quantity` — knowing both order and product determines quantity
- `course_id → instructor_name` — each course has one assigned instructor

A **partial dependency** exists when a non-key column depends on only *part* of a composite primary key — not the whole key.

A **transitive dependency** exists when a non-key column `C` depends on another non-key column `B`, which in turn depends on the key `A`: `A → B → C`.

## First Normal Form (1NF)

**Rule:** Every column must contain atomic (indivisible) values. No repeating groups. The table must have a primary key.

### Violation Example

| student_id | student_name | courses                   |
|-----------|--------------|---------------------------|
| 1         | Alice        | Math, English, Music      |
| 2         | Bob          | Physics, Chemistry        |

The `courses` column stores multiple values in a single cell. This violates atomicity. Queries like `WHERE courses = 'Math'` become fragile string searches.

### Corrected to 1NF

Split the multi-valued cell into separate rows, one per course. Add a composite primary key `(student_id, course)`:

| student_id | student_name | course    |
|-----------|--------------|-----------|
| 1         | Alice        | Math      |
| 1         | Alice        | English   |
| 1         | Alice        | Music     |
| 2         | Bob          | Physics   |
| 2         | Bob          | Chemistry |

!!! warning "Repeating column groups also violate 1NF"
    A table with columns `phone_1`, `phone_2`, `phone_3` has the same structural problem — it encodes a collection as fixed columns. Correct it by moving phone numbers to a separate `contact_phones` table with a foreign key back to the parent.

## Second Normal Form (2NF)

**Rule:** Must be in 1NF, and every non-key column must depend on the **entire** primary key — not just part of it. Partial dependencies are prohibited.

2NF only applies to tables with a **composite primary key**. A table with a single-column primary key is automatically in 2NF (there is no partial key to depend on).

### Violation Example

Consider an exam registration table with composite primary key `(candidate_id, subject_id)`:

```sql
CREATE TABLE exam_registration (
    candidate_id   INT,
    subject_id     INT,
    candidate_name VARCHAR(100),   -- depends only on candidate_id
    subject_fee    DECIMAL(10,2),  -- depends only on subject_id
    exam_date      DATE,           -- depends on both (full key)
    PRIMARY KEY (candidate_id, subject_id)
);
```

Functional dependencies:

- `candidate_id → candidate_name` — partial dependency (only part of the key)
- `subject_id → subject_fee` — partial dependency (only part of the key)
- `(candidate_id, subject_id) → exam_date` — full dependency (correct)

The partial dependencies mean: if a candidate's name changes, you update potentially hundreds of rows. If a subject fee changes, same problem.

### Corrected to 2NF

Decompose into three tables, each with columns fully dependent on their key:

```sql
CREATE TABLE candidates (
    candidate_id   INT PRIMARY KEY,
    candidate_name VARCHAR(100)
);

CREATE TABLE subjects (
    subject_id  INT PRIMARY KEY,
    subject_fee DECIMAL(10,2)
);

CREATE TABLE exam_registration (
    candidate_id INT,
    subject_id   INT,
    exam_date    DATE,
    PRIMARY KEY (candidate_id, subject_id),
    FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
    FOREIGN KEY (subject_id)   REFERENCES subjects(subject_id)
);
```

Now `candidate_name` exists in exactly one row in `candidates`. Changing it requires one `UPDATE` on one row.

## Third Normal Form (3NF)

**Rule:** Must be in 2NF, and no non-key column may depend on another non-key column. Transitive dependencies are prohibited.

### Violation Example

A course registration table where the primary key is `student_id`:

```sql
CREATE TABLE course_registration (
    student_id        INT PRIMARY KEY,
    course            VARCHAR(100),   -- depends on student_id
    instructor        VARCHAR(100),   -- depends on student_id (via course)
    instructor_office VARCHAR(20)     -- depends on instructor, NOT student_id
);
```

The transitive chain is:

```
student_id → course → instructor → instructor_office
```

`instructor_office` depends on `instructor`, not on `student_id`. This is a transitive dependency. If an instructor moves offices, every student enrolled in their course requires an update.

### Corrected to 3NF

Break the transitive chain:

```sql
CREATE TABLE course_registration (
    student_id  INT PRIMARY KEY,
    course      VARCHAR(100),
    instructor  VARCHAR(100),
    FOREIGN KEY (instructor) REFERENCES instructors(instructor_name)
);

CREATE TABLE instructors (
    instructor_name   VARCHAR(100) PRIMARY KEY,
    instructor_office VARCHAR(20)
);
```

Now `instructor_office` lives in exactly one row. One `UPDATE` covers the entire database.

!!! tip "Boyce-Codd Normal Form (BCNF)"
    BCNF is a slightly stricter variant of 3NF that handles edge cases involving overlapping candidate keys. For most practical OLTP design work, achieving 3NF is sufficient. BCNF is worth studying if your schema has multiple overlapping composite candidate keys.

## Normal Forms Summary

| Normal Form | Added Constraint | Eliminates |
|-------------|-----------------|------------|
| **1NF** | Atomic values, no repeating groups, primary key | Multi-value cells, repeating column groups |
| **2NF** | No partial dependency on composite key | Update anomalies from partial key columns |
| **3NF** | No transitive dependency | Update anomalies from non-key-to-non-key dependencies |

## Denormalization

Denormalization is the deliberate reversal of normalization — re-combining data that was separated, or pre-computing joins into wider tables. It trades storage space (and some update complexity) for query speed.

### Why Analytical Workloads Denormalize

In a normalized schema, answering "total revenue by product category by region for the last 12 months" requires joining five or more tables. At analytical scale — hundreds of millions of rows — each join is an expensive shuffle across a distributed cluster. The cost compounds with every additional join.

Denormalization eliminates these joins at query time by doing them once at load time:

- A customer dimension table pre-joins `customers`, `addresses`, and `customer_segments` into one flat table
- A product dimension pre-joins `products`, `categories`, and `suppliers`
- The fact table references these flat dimensions by foreign key — one join, not five

!!! info "Join cost vs. storage cost"
    In the era of cheap columnar cloud storage (S3, ADLS), storing a customer's city on every order line is trivially inexpensive. The cost of recomputing that join at query time across billions of rows is not. Analytical modeling deliberately chooses storage overhead to eliminate compute overhead at query time.

### The Trade-off

| Concern | Normalized (OLTP) | Denormalized (OLAP) |
|---------|-------------------|---------------------|
| **Storage** | Minimal — each fact stored once | Higher — context repeated per fact row |
| **Write performance** | Fast — small, targeted updates | Slower — ETL must re-flatten on change |
| **Read performance** | Slow for wide queries — many joins | Fast — pre-joined wide tables |
| **Data consistency** | Strong — one source of truth per fact | Weaker — duplicated context must be refreshed |
| **Update anomaly risk** | Low | Higher — stale denormalized data is possible |

### When to Normalize vs. Denormalize

- **Normalize** when the system is write-heavy, when data integrity is the top priority, and when the consumer is an application or API making single-row lookups. OLTP databases, microservice data stores, and operational APIs are normalize-first systems.

- **Denormalize** when the workload is read-heavy, when queries scan large ranges, and when the consumer is a BI tool, dashboard, or analyst running aggregations. Data warehouses, data marts, and analytical Gold layers in a Medallion architecture are denormalize-first systems.

!!! tip "Normalization in the Medallion architecture"
    In practice, the two approaches coexist. Bronze and Silver layers maintain normalized or near-normalized structures for CDC compatibility and auditability. Gold layers are deliberately denormalized into star schemas or wide tables to maximize query performance for analysts.
