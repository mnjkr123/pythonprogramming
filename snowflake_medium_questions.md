# Snowflake — 15 Medium Programming Questions & Detailed Answers

This document contains 15 medium-difficulty Snowflake programming questions with clear, practical solutions including SQL examples, Snowflake-specific features, and short explanations.

---

Q1 — Load semi-structured JSON from stage and extract nested fields

Problem
- You have newline-delimited JSON files in an internal stage. Each record has nested objects and arrays. Load into a table with extracted fields and preserve the variant.

Solution
- Create table and file format, then COPY INTO using the VARIANT column and SELECT to extract nested fields:

```sql
CREATE OR REPLACE FILE FORMAT my_json_fmt TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE TABLE orders_raw (raw VARIANT);

COPY INTO orders_raw
  FROM @my_stage/path/
  FILE_FORMAT = (FORMAT_NAME = 'my_json_fmt')
  ON_ERROR = 'CONTINUE';

-- Extract nested fields
CREATE OR REPLACE TABLE orders AS
SELECT
  raw:id::STRING AS order_id,
  raw:customer.id::STRING AS customer_id,
  raw:customer.name::STRING AS customer_name,
  raw:items AS items_variant,
  raw:total::NUMBER AS total
FROM orders_raw;

-- Flatten items array
SELECT
  o.order_id,
  f.value:sku::STRING AS sku,
  f.value:qty::NUMBER AS qty
FROM orders o,
LATERAL FLATTEN(input => o.items_variant) f;
```

Explanation
- Use a VARIANT column to store raw JSON, then type-cast paths using the colon syntax. `LATERAL FLATTEN` expands arrays. Keep raw variant for auditing.

---

Q2 — Implement slowly changing dimension (SCD Type 2) using MERGE

Problem
- Maintain `dim_customer` as SCD Type 2 with effective start/end timestamps when loading a changed records file.

Solution

```sql
-- staging_customer has incoming snapshot with natural_key and attributes
MERGE INTO dim_customer T
USING (
  SELECT *, CURRENT_TIMESTAMP() AS load_ts FROM staging_customer
) S
ON T.natural_key = S.natural_key AND T.end_ts IS NULL
WHEN MATCHED AND (
     (T.attr1 IS DISTINCT FROM S.attr1) OR
     (T.attr2 IS DISTINCT FROM S.attr2)
  ) THEN
  UPDATE SET end_ts = S.load_ts
WHEN NOT MATCHED THEN
  INSERT (natural_key, attr1, attr2, start_ts, end_ts)
  VALUES (S.natural_key, S.attr1, S.attr2, S.load_ts, NULL);

-- Insert new current rows for those changed
INSERT INTO dim_customer (natural_key, attr1, attr2, start_ts, end_ts)
SELECT S.natural_key, S.attr1, S.attr2, S.load_ts, NULL
FROM staging_customer S
LEFT JOIN dim_customer D
  ON D.natural_key = S.natural_key AND D.end_ts IS NULL
WHERE D.natural_key IS NULL OR (
      D.attr1 IS DISTINCT FROM S.attr1 OR
      D.attr2 IS DISTINCT FROM S.attr2
);
```

Explanation
- Use `MERGE` to close existing current rows by setting `end_ts`. Then insert new current rows. `IS DISTINCT FROM` handles NULL-aware comparisons.

---

Q3 — Stream + Task pattern: incremental ingest from staged files

Problem
- Use Snowflake Streams and Tasks to apply new staged CSV files (COPY INTO staging table) into a target table incrementally.

Solution

1. Create a staging table and load raw files with COPY INTO. Create a stream on staging

```sql
CREATE OR REPLACE TABLE raw_events (payload VARIANT, src_file STRING, load_ts TIMESTAMP);
CREATE OR REPLACE STREAM raw_events_stream ON TABLE raw_events;

-- task that runs COPY into raw_events periodically (or triggered externally)
CREATE OR REPLACE TASK task_load_raw
  WAREHOUSE = my_wh
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  COPY INTO raw_events FROM @my_stage/ FILE_FORMAT = (TYPE = 'CSV');

-- Task that processes the stream into curated table
CREATE OR REPLACE TASK task_process_stream
  WAREHOUSE = my_wh
  AFTER task_load_raw
AS
  INSERT INTO events SELECT payload:col1::STRING, payload:col2::NUMBER FROM raw_events_stream WHERE METADATA$ISROWDELETED = FALSE;

-- enable tasks
ALTER TASK task_load_raw RESUME;
ALTER TASK task_process_stream RESUME;
```

Explanation
- Streams capture DML changes on the staging table; Tasks run scheduled pipelines. Use `METADATA$ISROWDELETED` to detect inserts vs deletes (if needed).

---

Q4 — Efficiently retrieve top-N per group using window functions

Problem
- Return top 3 orders by amount per customer from `orders` table.

Solution

```sql
SELECT order_id, customer_id, total
FROM (
  SELECT order_id, customer_id, total,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total DESC) AS rn
  FROM orders
)
WHERE rn <= 3;
```

Explanation
- Use `ROW_NUMBER()` partitioned by customer and order by total descending. This pattern is efficient and idiomatic.

---

Q5 — Querying semi-structured data with conditional logic

Problem
- JSON `events` column has variable fields. Compute `event_type` with fallback logic and extract nested value when present.

Solution

```sql
SELECT
  e:id::STRING AS id,
  COALESCE(e:type::STRING, e:meta.type::STRING, 'unknown') AS event_type,
  CASE
    WHEN e:event.payload IS NOT NULL THEN e:event.payload:data::STRING
    ELSE NULL
  END AS payload_data
FROM raw_events;
```

Explanation
- Use `COALESCE` + variant path extraction. Use `CASE` to avoid runtime errors accessing absent paths.

---

Q6 — Time travel and cloning for safe data repair

Problem
- Accidentally deleted rows; restore state from time-travel to a new table for comparison and recovery.

Solution

```sql
-- create clone from 1 hour ago
CREATE TABLE orders_restore AS
  SELECT * FROM orders AT (TIMESTAMP => DATEADD(hour, -1, CURRENT_TIMESTAMP()));

-- compare or merge back missing rows
MERGE INTO orders T
USING orders_restore S
ON T.order_id = S.order_id
WHEN NOT MATCHED THEN INSERT *;
```

Explanation
- Time travel allows querying past data; cloning into a new table is zero-copy and fast. Use `MERGE` to recover missing rows.

---

Q7 — Implement a stored procedure in Snowflake using Snowpark Python to deduplicate and write results

Problem
- Use Snowpark Python procedure to deduplicate a table by natural key and write the deduped result into a target table.

Solution

```sql
CREATE OR REPLACE PROCEDURE dedupe_table(src_table STRING, dst_table STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
AS
$$
def run(session, src_table, dst_table):
    df = session.table(src_table)
    # keep latest by event_ts
    windowed = df.with_column('rn', F.row_number().over(F.Window.partition_by('natural_key').order_by(F.col('event_ts').desc())))
    deduped = windowed.filter(F.col('rn') == 1).drop('rn')
    deduped.write.save_as_table(dst_table, mode='overwrite')
    return 'OK'
$$;

CALL dedupe_table('raw_src', 'curated_dst');
```

Explanation
- Snowpark Python lets you express complex logic with DataFrame APIs server-side. Use window functions and save result to a table.

---

Q8 — Optimize large table joins: clustering keys and pruning

Problem
- Joining a very large fact table to dimension filtered by date range is slow. Suggest improvements.

Solution

Guidance:
- Use proper clustering on the fact table (e.g., `CLUSTER BY (event_date)`) to improve pruning.
- Ensure filters are on clustered columns. Use micro-partition pruning (verified via `QUERY_HISTORY`/`QUERY_PROFILE`).
- Consider using a materialized view for frequently-run filtered aggregation.

Example:

```sql
ALTER TABLE fact_events CLUSTER BY (event_date);

CREATE MATERIALIZED VIEW mv_daily AS
SELECT event_date, COUNT(*) AS cnt FROM fact_events GROUP BY event_date;
```

Explanation
- Clustering improves physical pruning; materialized views precompute aggregates.

---

Q9 — Use `FLATTEN` + windowing to compute metrics across nested arrays

Problem
- Each `session` record contains an array of `actions`. Compute total actions and top action per session.

Solution

```sql
SELECT s.session_id,
       COUNT(a.value) AS total_actions,
       ARRAY_AGG(a.value ORDER BY a.value_count DESC)[0] AS top_action
FROM (
  SELECT session_id, f.value AS action
  FROM sessions,
       LATERAL FLATTEN(input => sessions.actions) f
) a
GROUP BY s.session_id;
```

Explanation
- Flatten then aggregate. For more complex top-K use `ROW_NUMBER()` over partition.

---

Q10 — COPY performance tuning for bulk CSV loads

Problem
- Loading multi-GB CSV files to Snowflake is slow. What settings and patterns improve speed?

Solution

Recommendations:
- Compress files (gzip) before staging.
- Increase `MAX_CONCURRENCY_LEVEL` and use a larger warehouse size for COPY.
- Use `PURGE = TRUE` only after successful loads to avoid repeated processing.
- Use consistent `FILE_FORMAT` settings (e.g., `SKIP_HEADER`, `FIELD_OPTIONALLY_ENCLOSED_BY`).

Example COPY:

```sql
COPY INTO my_table
FROM @my_stage/bulk/
FILE_FORMAT = (type='CSV' compression='GZIP' field_delimiter=',' skip_header=1)
ON_ERROR='ABORT_STATEMENT'
MAX_CONCURRENCY_LEVEL = 8;
```

Explanation
- Compression reduces network IO. Larger warehouses and parallelism speed ingestion.

---

Q11 — Implement multi-table atomic update with transactions

Problem
- Update multiple related tables atomically (if any update fails, rollback all changes).

Solution

```sql
BEGIN TRANSACTION;
  UPDATE acct_balances SET balance = balance - 100 WHERE acct_id = 1;
  UPDATE acct_balances SET balance = balance + 100 WHERE acct_id = 2;
  INSERT INTO transfers (from_acct, to_acct, amount, ts) VALUES (1,2,100,CURRENT_TIMESTAMP());
COMMIT;

-- In case of error, use ROLLBACK;
```

Explanation
- Snowflake supports multi-statement transactions; use `BEGIN`/`COMMIT` to ensure atomicity.

---

Q12 — Use `QUALIFY` to simplify filtering on analytic results

Problem
- Filter rows by the output of a window function (e.g., top 1 per group) without subquery nesting.

Solution

```sql
SELECT order_id, customer_id, total,
       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total DESC) AS rn
FROM orders
QUALIFY rn = 1;
```

Explanation
- `QUALIFY` lets you filter on analytic/window expressions directly, reducing subquery complexity.

---

Q13 — Use `MERGE` to implement upsert with delete semantics (synchronization)

Problem
- Synchronize target table to match source: insert new, update changed, and delete target rows not present in source.

Solution

```sql
MERGE INTO target T
USING source S
ON T.key = S.key
WHEN MATCHED THEN UPDATE SET T.col1 = S.col1, T.col2 = S.col2
WHEN NOT MATCHED THEN INSERT (key, col1, col2) VALUES (S.key, S.col1, S.col2)
WHEN NOT MATCHED BY SOURCE AND T.update_source = 'external' THEN DELETE;
```

Explanation
- `MERGE` supports `WHEN NOT MATCHED BY SOURCE` to delete rows that no longer exist upstream; use carefully to avoid accidental data loss.

---

Q14 — Implement GDPR-style soft deletion using masking + time ranges

Problem
- Support soft-deletion for PII data and automatically mask values for deleted users while retaining analytics.

Solution

Approach:
- Add `deleted_at` timestamp column. Use conditional masking in views: if `deleted_at` IS NOT NULL then mask PII columns using `HASH()` or `NULL`.

```sql
CREATE OR REPLACE VIEW customer_view AS
SELECT id,
       CASE WHEN deleted_at IS NULL THEN email ELSE NULL END AS email,
       CASE WHEN deleted_at IS NULL THEN ssn ELSE 'REDACTED' END AS ssn,
       deleted_at
FROM customer;
```

Explanation
- Views provide controlled access—masking PII while retaining row-level metrics.

---

Q15 — Use materialized view to accelerate frequent aggregation with freshness constraints

Problem
- A dashboard queries daily aggregates frequently; reduce latency while keeping results near-real-time.

Solution

```sql
CREATE MATERIALIZED VIEW mv_daily_sales
CLUSTER BY (sale_date)
AS
SELECT sale_date, SUM(amount) AS total_sales, COUNT(*) AS tx_count
FROM sales
GROUP BY sale_date;

-- Refresh occurs automatically; monitor via ACCOUNT_USAGE.MATERIALIZED_VIEWS
```

Explanation
- Materialized views precompute aggregates; they trade storage for query latency. Monitor maintenance cost and invalidation frequency.

---

File created: `snowflake_medium_questions.md`

If you want, I can also:
- Convert this to a notebook cell, or
- Attempt syntactic validation of these SQL snippets if you provide Snowflake credentials.
