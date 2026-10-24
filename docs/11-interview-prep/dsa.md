# Data Structures & Algorithms

This page covers the algorithms and data structure patterns that appear most frequently in data engineering interviews. This is not full LeetCode preparation — it is targeted at the patterns that interviewers use to evaluate whether you can reason about computational complexity in the context of large-scale data systems.

!!! info "Scope"
    DE interviews at product companies test DSA to verify software engineering fundamentals. You will rarely be asked to implement a graph traversal — but you will be asked about hash map lookup, sorting complexity, and why `groupByKey` can be catastrophic at scale. This page bridges both worlds.

---

## Big O Complexity for Data Engineers

### Complexity Hierarchy

Understanding where an algorithm sits in the complexity hierarchy tells you whether it will scale to millions or billions of rows.

```
O(1) < O(log N) < O(N) < O(N log N) < O(N²) < O(2^N)
```

| Complexity | Name | N = 1,000 ops | N = 1,000,000 ops | Example |
|---|---|---|---|---|
| O(1) | Constant | 1 | 1 | Hash map lookup, array index access |
| O(log N) | Logarithmic | 10 | 20 | Binary search on sorted array |
| O(N) | Linear | 1,000 | 1,000,000 | Single array scan, `filter()` in Spark |
| O(N log N) | Linearithmic | 10,000 | 20,000,000 | Merge sort, sort-merge join in Spark |
| O(N²) | Quadratic | 1,000,000 | 10^12 — infeasible | Nested loop join, brute-force two-sum |

### Applied to Spark Operations

| Spark Operation | Complexity | Notes |
|---|---|---|
| `filter()`, `select()`, `withColumn()` | O(N) | Single pass over partitions — narrow transformation |
| `sort()` / `orderBy()` | O(N log N) | Requires full shuffle + sort |
| `groupBy().agg()` with hash aggregation | O(N) | Hash table aggregation on each executor |
| `groupByKey()` (RDD) | O(N) → O(N²) if memory exceeded | Shuffles all values per key to one executor; can OOM |
| Sort-merge join | O(N log N) | Both sides must be sorted — shuffle + sort |
| Broadcast hash join | O(N) | Probe side scanned once; build side in memory |
| `distinct()` | O(N log N) | Requires sort or hash across shuffle |
| Nested loop join (Cartesian) | O(N × M) | Never use on large tables |

!!! warning "groupByKey at Scale"
    `groupByKey()` on a large RDD shuffles all values for each key to a single reducer. If a key has millions of values, that reducer runs out of memory. Always prefer `reduceByKey()` or `aggregateByKey()`, which perform partial aggregation (combiner phase) on each partition before the shuffle — equivalent to the combiner in MapReduce.

---

## Array & Hashing Patterns

### Two Sum — Hash Map Approach

**Problem:** Find the indices of two numbers in an array that add up to a target.

**Why it matters for DE:** Demonstrates the fundamental hash map pattern — O(N) lookup by pre-computing complements. The same pattern applies to deduplication, lookup joins, and change detection.

```python
# O(N) time, O(N) space — single-pass hash map
# The brute-force nested loop approach is O(N²) — never acceptable at scale.

from typing import List

def two_sum(nums: List[int], target: int) -> List[int]:
    seen = {}  # value -> index

    for i, num in enumerate(nums):
        complement = target - num
        if complement in seen:          # O(1) hash map lookup
            return [seen[complement], i]
        seen[num] = i                   # store current value
    return []

# Example: nums=[2,7,11,15], target=9 → [0,1] (2+7=9)
```

**Spark analogy:** A broadcast join is the distributed equivalent — the smaller table is loaded into a hash map in each executor, then the larger table is scanned once for O(N) total joins rather than O(N×M).

---

### Single Number — XOR Trick

**Problem:** Find the element that appears exactly once in an array where all other elements appear twice.

**Why it matters for DE:** Demonstrates bitwise operations and O(1) space — relevant to deduplication and checksum verification. Also a classic interview question for demonstrating cleverness under constraints.

```python
# O(N) time, O(1) space — XOR properties:
# x XOR x = 0 (duplicates cancel)
# x XOR 0 = x (unique element survives)
# XOR is commutative and associative — order doesn't matter

def single_number(nums: List[int]) -> int:
    result = 0
    for num in nums:
        result ^= num   # XOR each element; duplicates cancel to 0
    return result       # only the unique element remains

# Example: [4,1,2,1,2] → XOR chain: 4^1^2^1^2 = 4
```

---

### Majority Element — Boyer-Moore Voting

**Problem:** Find the element that appears more than ⌊N/2⌋ times.

**Why it matters for DE:** The Boyer-Moore algorithm is a streaming algorithm — it processes the array in a single pass with O(1) space. The same concept underlies approximate count-distinct algorithms (HyperLogLog) used in Spark's `approx_count_distinct()`.

```python
# O(N) time, O(1) space
# Intuition: the majority element "wins" every vote cancellation.
# Candidate is maintained; count increments for matches, decrements for mismatches.
# When count hits 0, the current element becomes the new candidate.

def majority_element(nums: List[int]) -> int:
    candidate = None
    count = 0

    for num in nums:
        if count == 0:
            candidate = num          # elect new candidate
        count += (1 if num == candidate else -1)

    return candidate  # guaranteed to exist per problem constraints

# Example: [3,2,3] → candidate=3, [2,1,2,1,1] → candidate=1
```

---

### Maximum Subarray — Kadane's Algorithm

**Problem:** Find the contiguous subarray with the maximum sum.

**Why it matters for DE:** Kadane's is a classic dynamic programming pattern that appears in sliding window aggregations and running maximum calculations. The same structure is used in streaming tumbling window computations.

```python
# O(N) time, O(1) space
# Key insight: if the running sum drops below 0, reset it to 0
# (a negative prefix only makes the total sum worse — discard it).

def max_sub_array(nums: List[int]) -> int:
    max_sum = nums[0]        # best seen so far
    current_sum = 0          # running window sum

    for num in nums:
        if current_sum < 0:
            current_sum = 0  # discard negative prefix
        current_sum += num
        if current_sum > max_sum:
            max_sum = current_sum

    return max_sum

# Example: [-2,1,-3,4,-1,2,1,-5,4] → max subarray [4,-1,2,1] = 6
```

---

### Rotate Array — Three-Reversal Trick

**Problem:** Rotate an array to the right by k positions in place.

**Why it matters for DE:** Demonstrates in-place manipulation and the reversal decomposition trick — the same approach is used in block-level operations on partitioned datasets.

```python
# O(N) time, O(1) space
# Three reversals: reverse all → reverse first k → reverse remaining

def rotate(nums: List[int], k: int) -> None:
    n = len(nums)
    k %= n  # handle k > n (k=7, n=3 is same as k=1)

    def reverse(start: int, end: int) -> None:
        while start < end:
            nums[start], nums[end] = nums[end], nums[start]
            start += 1
            end -= 1

    reverse(0, n - 1)    # reverse entire array
    reverse(0, k - 1)    # reverse first k elements
    reverse(k, n - 1)    # reverse remaining elements

# Example: [1,2,3,4,5,6,7], k=3 → [5,6,7,1,2,3,4]
```

---

## Linked List Patterns

### Reverse a Linked List — Iterative Three-Pointer

**Problem:** Reverse a singly linked list.

**Why it matters for DE:** Tests pointer manipulation and in-place operations. Interviewers use this to verify you can reason about mutation and state — important for understanding Spark's immutable RDD model by contrast.

```python
# O(N) time, O(1) space — three-pointer iterative approach.
# For recursive approach: O(N) time, O(N) space (call stack).

class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next

def reverse_list_iterative(head: ListNode) -> ListNode:
    prev = None
    curr = head

    while curr is not None:
        temp_next = curr.next   # save next before overwriting
        curr.next = prev        # reverse the pointer
        prev = curr             # advance prev
        curr = temp_next        # advance curr

    return prev  # prev is now the new head

# Recursive solution (O(N) stack space):
def reverse_list_recursive(head: ListNode) -> ListNode:
    if head is None or head.next is None:
        return head  # base case: empty list or single node
    new_head = reverse_list_recursive(head.next)  # recurse to end
    head.next.next = head   # reverse pointer
    head.next = None        # sever forward link
    return new_head
```

---

### Find Middle of Linked List — Floyd's Fast/Slow Pointer

**Problem:** Find the middle node of a singly linked list in a single pass.

**Why it matters for DE:** The fast/slow pointer (Floyd's algorithm) is the foundation for cycle detection in graphs and linked structures — used in detecting circular dependencies in DAGs and pipeline dependency loops.

```python
# O(N) time, O(1) space
# Fast moves 2 steps, slow moves 1 step.
# When fast reaches the end, slow is at the middle.
# For even-length lists, slow lands on the second of the two middle nodes.

def find_middle(head: ListNode) -> ListNode:
    slow = head
    fast = head

    while fast is not None and fast.next is not None:
        slow = slow.next        # advance 1 step
        fast = fast.next.next   # advance 2 steps

    return slow  # slow is at the middle when fast reaches end

# Example: [1,2,3,4,5] → middle = 3
#          [1,2,3,4]   → middle = 3 (second of [2,3])
```

---

## SQL & Data Patterns

These patterns appear in nearly every DE technical screen. Know them cold.

### Window Functions

```sql
-- ROW_NUMBER: unique sequential rank per partition
SELECT
    customer_id,
    order_id,
    order_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
FROM orders;
-- Use: get the most recent order per customer (WHERE rn = 1)

-- RANK: rank with gaps for ties
-- DENSE_RANK: rank without gaps for ties
SELECT
    product_id,
    sales_amount,
    RANK()       OVER (ORDER BY sales_amount DESC) AS rank_with_gaps,
    DENSE_RANK() OVER (ORDER BY sales_amount DESC) AS rank_no_gaps
FROM daily_sales;
-- RANK:       1, 2, 2, 4   (gap after tie)
-- DENSE_RANK: 1, 2, 2, 3   (no gap)

-- LAG / LEAD: access a previous or next row's value
SELECT
    order_date,
    revenue,
    LAG(revenue,  1, 0) OVER (ORDER BY order_date) AS prev_day_revenue,
    LEAD(revenue, 1, 0) OVER (ORDER BY order_date) AS next_day_revenue,
    revenue - LAG(revenue, 1, 0) OVER (ORDER BY order_date) AS day_over_day_delta
FROM daily_revenue;
```

### Running Totals

```sql
-- Running total using SUM() OVER with ORDER BY
SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (ORDER BY order_date
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            ) AS running_total
FROM daily_revenue;

-- 7-day moving average
SELECT
    order_date,
    daily_revenue,
    AVG(daily_revenue) OVER (ORDER BY order_date
                             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                            ) AS moving_avg_7d
FROM daily_revenue;
```

### Finding Duplicates

```sql
-- GROUP BY + HAVING: find duplicate keys
SELECT customer_id, COUNT(*) AS cnt
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Using window function to tag duplicates (then filter or delete)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
    FROM customers
)
WHERE rn > 1;  -- rows 2, 3, ... are duplicates
```

### Self-Join for Hierarchy Traversal

```sql
-- Employee → Manager hierarchy (one level up)
SELECT
    e.employee_id,
    e.employee_name,
    m.employee_name AS manager_name
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id;

-- Recursive CTE for multi-level hierarchy (standard SQL)
WITH RECURSIVE org_tree AS (
    -- Anchor: top-level employees (no manager)
    SELECT employee_id, employee_name, manager_id, 0 AS depth
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive: join employees to their managers
    SELECT e.employee_id, e.employee_name, e.manager_id, ot.depth + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.employee_id
)
SELECT * FROM org_tree ORDER BY depth, manager_id;
```

### Finding Gaps in a Sequence

```sql
-- Find missing IDs in a sequence using LAG
SELECT
    id + 1 AS gap_start,
    next_id - 1 AS gap_end
FROM (
    SELECT
        id,
        LEAD(id, 1) OVER (ORDER BY id) AS next_id
    FROM orders
)
WHERE next_id - id > 1;

-- Example: IDs [1,2,4,7] → gaps at [3,3] and [5,6]
```

### Median Calculation

```sql
-- Exact median in standard SQL using PERCENTILE_CONT
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees;

-- In Spark SQL / PySpark:
-- percentile_approx is faster for large datasets (uses reservoir sampling)
SELECT percentile_approx(salary, 0.5) AS median_salary
FROM employees;

-- PySpark DataFrame API:
from pyspark.sql.functions import percentile_approx
df.agg(percentile_approx("salary", 0.5).alias("median_salary"))
```

---

## Bloom Filters — Probabilistic Membership

A Bloom filter is a space-efficient probabilistic data structure that answers the question "is this element definitely NOT in the set, or is it possibly in the set?" with zero false negatives and controlled false positives.

```
Input key → Hash Function 1 → Bit position 2
           → Hash Function 2 → Bit position 5
           → Hash Function 3 → Bit position 9

Bit Array: [0|0|1|0|0|1|0|0|0|1|0|0]
                ↑       ↑       ↑
```

**Properties:**
- **False positives are possible** — multiple keys can map to the same bit positions.
- **False negatives are impossible** — if any mapped bit is 0, the element is definitively absent.
- Space complexity is O(1) relative to set size — the bit array is fixed size.

**Data engineering use cases:**
- Spark's shuffle join uses a Bloom filter to skip sending non-matching rows across the network (`spark.sql.optimizer.runtime.bloomFilter.enabled`).
- Delta Lake can use Bloom filters on high-cardinality columns to skip reading row groups that cannot contain the query predicate value.
- Databases use Bloom filters to avoid expensive disk lookups for keys that don't exist (e.g., RocksDB in Kafka's state store).

```python
# Conceptual Python implementation (for illustration, not production)
import hashlib

class BloomFilter:
    def __init__(self, size: int = 1024, num_hashes: int = 3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [0] * size

    def _hashes(self, item: str):
        """Generate num_hashes different hash positions for item."""
        for seed in range(self.num_hashes):
            h = int(hashlib.md5(f"{seed}:{item}".encode()).hexdigest(), 16)
            yield h % self.size

    def add(self, item: str) -> None:
        for pos in self._hashes(item):
            self.bit_array[pos] = 1

    def possibly_contains(self, item: str) -> bool:
        """Returns False = definitely not in set. True = possibly in set."""
        return all(self.bit_array[pos] == 1 for pos in self._hashes(item))

# Usage:
bf = BloomFilter(size=2048, num_hashes=5)
bf.add("user_123")
bf.add("user_456")

print(bf.possibly_contains("user_123"))  # True (was added)
print(bf.possibly_contains("user_999"))  # False (definitely not added)
# Note: True is probabilistic — False is certain.
```

!!! tip "Interview Tip — Bloom Filters"
    If asked "how would you avoid expensive lookups for keys that don't exist?", mentioning Bloom filters signals genuine systems knowledge. Bonus points for connecting it to Spark's runtime Bloom filter join optimization or Delta Lake's column statistics skipping.

---

## Quick Reference — Complexity Cheat Sheet

| Algorithm / Pattern | Time | Space | Notes |
|---|---|---|---|
| Two Sum (hash map) | O(N) | O(N) | Single-pass, O(N²) brute force is wrong answer |
| Single Number (XOR) | O(N) | O(1) | XOR: duplicates cancel, unique survives |
| Majority Element (Boyer-Moore) | O(N) | O(1) | Streaming algorithm — one pass |
| Maximum Subarray (Kadane) | O(N) | O(1) | DP pattern — reset on negative prefix |
| Rotate Array (three-reversal) | O(N) | O(1) | In-place; naive copy approach is O(N) space |
| Reverse Linked List (iterative) | O(N) | O(1) | Recursive uses O(N) stack |
| Find Middle (fast/slow pointer) | O(N) | O(1) | One pass — no length pre-computation needed |
| Binary Search | O(log N) | O(1) | Requires sorted input |
| Merge Sort | O(N log N) | O(N) | Stable; basis for Spark sort-merge join |
| Hash Aggregation (groupBy) | O(N) avg | O(K) | K = number of distinct keys |
| Sort-Merge Join (Spark) | O(N log N) | O(N) | Both sides sorted; shuffled to matching reducers |
| Broadcast Hash Join (Spark) | O(N + M) | O(M) | M = small side; no shuffle on large side |
| Bloom Filter lookup | O(k) | O(m) | k = hash functions, m = bit array size |

---

## Hash Maps in Depth

Hash maps (Python `dict`) underpin many of the most efficient DE algorithms. Understanding their mechanics explains why so many O(N²) brute-force solutions can be reduced to O(N).

### How Hash Map Lookup Achieves O(1)

A hash map uses a hash function to map each key to a bucket index. Lookup, insert, and delete are O(1) average case because the bucket index is computed directly rather than searched:

```python
# O(1) average lookup — no iteration required
phone_book = {"Alice": "555-1234", "Bob": "555-5678"}
number = phone_book["Alice"]   # hash("Alice") → bucket index → direct access

# O(N) lookup in a list — must scan every element
numbers = [("Alice", "555-1234"), ("Bob", "555-5678")]
for name, num in numbers:       # O(N) — must scan all entries
    if name == "Alice":
        number = num
        break
```

**Hash collision:** When two keys hash to the same bucket, the map resolves the collision (via chaining or open addressing). Worst-case with many collisions: O(N). In practice, well-designed hash functions keep collision rates low enough that O(1) is the effective complexity.

### Frequency Counter Pattern

One of the most common interview patterns — use a hash map to count occurrences in O(N):

```python
# O(N) time, O(K) space — K = number of distinct elements
# Applied in DE: count events per category, detect rare vs. common values

from collections import Counter

def find_top_k_frequent(nums: list, k: int) -> list:
    # Count frequencies in O(N)
    freq = Counter(nums)  # equivalent to: {val: count, ...}

    # Sort by frequency descending and take top k — O(K log K)
    return [val for val, _ in freq.most_common(k)]

# Example: nums=[1,1,1,2,2,3], k=2 → [1, 2]
# In Spark: df.groupBy("category").count().orderBy(desc("count")).limit(k)
```

### Two-Pointer Pattern

The two-pointer technique solves sorted-array problems in O(N) rather than O(N²):

```python
# Find if any two numbers in a SORTED array sum to target — O(N) time, O(1) space
def two_sum_sorted(numbers: list, target: int) -> list:
    left, right = 0, len(numbers) - 1

    while left < right:
        current_sum = numbers[left] + numbers[right]
        if current_sum == target:
            return [left + 1, right + 1]   # 1-indexed
        elif current_sum < target:
            left += 1    # need a larger sum — move left pointer right
        else:
            right -= 1   # need a smaller sum — move right pointer left

    return []

# Two-pointer in DE context: finding gaps between sequential timestamps,
# merging two sorted lists (used in external sort-merge join implementation)
```

---

## Binary Search

Binary search reduces lookup in a sorted structure from O(N) to O(log N) by halving the search space on each step.

```python
# O(log N) time, O(1) space
# Prerequisite: array must be SORTED

def binary_search(nums: list, target: int) -> int:
    left, right = 0, len(nums) - 1

    while left <= right:
        mid = left + (right - left) // 2  # avoids integer overflow vs (left+right)//2

        if nums[mid] == target:
            return mid          # found
        elif nums[mid] < target:
            left = mid + 1      # target is in the right half
        else:
            right = mid - 1     # target is in the left half

    return -1  # not found

# Example: binary_search([1,3,5,7,9,11], 7) → 3 (index)
```

**DE applications of binary search:**
- Parquet's row group statistics (min/max per column) enable the query engine to perform binary search-like skipping across sorted row groups.
- Delta Lake's Z-Order co-locates data to make file-level skipping more effective — conceptually, the engine is doing multi-dimensional binary search over the file manifest.
- Time-series lookups: find all records for a given date range in a sorted time-partitioned table.

---

## Sliding Window Pattern

Sliding window solves problems that ask for the maximum or minimum over a contiguous subrange without O(N²) nested loops.

```python
# Maximum sum of any subarray of length k — O(N) time, O(1) space
def max_sum_subarray(nums: list, k: int) -> int:
    if len(nums) < k:
        return 0

    # Compute the sum of the first window
    window_sum = sum(nums[:k])   # O(k)
    max_sum = window_sum

    # Slide the window: add next element, remove leftmost element
    for i in range(k, len(nums)):
        window_sum += nums[i] - nums[i - k]   # O(1) per step
        max_sum = max(max_sum, window_sum)

    return max_sum

# Example: nums=[2,1,5,1,3,2], k=3 → max window [5,1,3] = 9
```

**DE applications:**
- Computing moving averages and rolling aggregations in streaming (`ROWS BETWEEN N PRECEDING AND CURRENT ROW`)
- Spark's `window` function for running totals and moving metrics implements this pattern internally
- Watermark-based window aggregations in Structured Streaming use a sliding window over event time

---

## Interview Tips for Coding Rounds

### Before You Code

1. **Restate the problem** in your own words and confirm with the interviewer.
2. **Ask about edge cases**: empty input, negative numbers, single element, duplicate values.
3. **State your approach** and complexity before writing a single line: "I'll use a hash map for O(N) time and O(N) space — is that acceptable?"
4. **Write a simple test case** on the side to verify your logic before coding.

### While You Code

- Use descriptive variable names (`complement` not `c`, `slow` not `s`).
- Narrate your thinking: "I'm storing the complement so I can check in O(1) on the next iteration."
- Do not erase and restart silently — explain why you're changing direction.

### After You Code

- Walk through your test case manually to verify correctness.
- State the time and space complexity explicitly: "This is O(N) time and O(N) space for the hash map."
- Proactively discuss trade-offs: "If we needed O(1) space, we could sort first and use two pointers, but that would be O(N log N) time."

!!! info "DE-Specific Coding Expectations"
    Data engineering interviews rarely ask for hard graph problems or dynamic programming. The most common patterns are: hash maps for deduplication/lookup, sliding windows for time-series aggregation, two pointers for sorted array problems, and SQL window functions for analytical query design. Master these five patterns and you will handle the majority of DE coding rounds.
