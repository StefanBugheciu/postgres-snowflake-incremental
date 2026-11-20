# Incremental pipeline interview preparation

## A 30-second walkthrough

I sync customers and orders from PostgreSQL. The first run reads the
available source rows, and later runs use a saved timestamp boundary
to read changed rows. Python sends each batch through Snowflake staging.
SQL MERGE upserts the analytics tables by ID. The same Snowflake
transaction commits the merges and the new watermark.

## Questions to practice

- **What is a watermark?** The upper timestamp of the last committed
  extraction window. I read the next window starting at that timestamp
  and exclude the new upper boundary.
- **What if timestamps tie?** All rows with the same timestamp satisfy
  the same range. I order by timestamp and ID, and integration-tested
  two changes at the exact inclusive lower boundary.
- **What about writes during extraction?** SHARE locks wait for
  in-flight writes and hold off new ones through the read. The upper
  bound comes from the PostgreSQL clock after acquiring those locks.
  The demo assumes the clock does not move backward.
- **When does the watermark move?** The target data and watermark
  share a transaction. A failed merge rolls back both. This keeps the
  old extraction boundary available on retry.
- **Why can the watermark advance on an empty run?** A successful
  query completed the next time window even though neither table
  returned rows. The boundary advances while the analytics tables
  keep the same contents.
- **How do you avoid duplicates?** Stable source IDs match rows in
  MERGE. The source advisory lock prevents two pipeline runs against
  the same source database from racing.
- **Is this log-based CDC?** This design polls current row timestamps.
  PostgreSQL WAL-based CDC captures committed change events and is
  better suited to tracking hard deletes and intermediate updates.
- **What failed in the live run and how did you fix it?** The
  connector tried to rewrite a parameterized `INSERT … SELECT`
  passed to `executemany` as a multi-row insert. This demo now
  executes one bound insert per source row before the merge.
  The retry confirmed the failed attempt had left the target and
  watermark empty.
- **What did you verify live?** The three runs transferred 3
  customers and 4 orders, then 2 customers and 0 orders, then no
  rows. Snowflake ended with 4 distinct IDs per entity, zero orphan
  orders, and a watermark that advanced after each successful window.
