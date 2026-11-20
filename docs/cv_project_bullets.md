# CV project bullets

- Built and validated a Python pipeline from PostgreSQL 17 to Snowflake that used an `updated_at` watermark and SQL MERGE to load 3 customers and 4 orders initially, then transfer only the 2 changed customers.
- Verified a zero-row repeat run, 4 distinct IDs per target table, zero orphan orders, and a persisted watermark. Tested that two rows with identical timestamps at the extraction boundary were both included.
