from datetime import datetime, timezone

import snow
from source import TABLES

DB = "INCREMENTAL_PIPELINE_DB"
WATERMARK = f"{DB}.CONTROL.WATERMARK"


def get_watermark(connection) -> datetime | None:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT UPPER_BOUND FROM {WATERMARK} WHERE STREAM = 'postgres_demo'")
        row = cursor.fetchone()
        return row[0] if row else None


def load_window(connection, upper: datetime, batch: dict[str, list[tuple]]) -> None:
    # Each sync opens a fresh Snowflake session. Create staging before BEGIN
    # because Snowflake DDL auto-commits.
    if any(batch.values()):
        snow.run_sql(connection, "staging.sql")

    with connection.cursor() as cursor:
        cursor.execute("BEGIN")
        try:
            for table, columns in TABLES.items():
                rows = batch[table]
                if rows:
                    names = ", ".join(columns)
                    placeholders = ", ".join(["%s"] * (len(columns) - 1) + ["TO_TIMESTAMP_TZ(%s)"])
                    insert = f"INSERT INTO {DB}.STAGING.BATCH_{table} ({names}) SELECT {placeholders}"
                    # The connector's executemany INSERT rewrite requires VALUES.
                    for row in rows:
                        values = (*row[:-1], row[-1].astimezone(timezone.utc).isoformat())
                        cursor.execute(insert, values)
                    sql = (snow.ROOT / "sql" / f"merge_{table}.sql").read_text(encoding="utf-8")
                    cursor.execute(sql)
            cursor.execute(f"""
                MERGE INTO {WATERMARK} AS target
                USING (
                    SELECT 'postgres_demo' AS STREAM, TO_TIMESTAMP_TZ(%s) AS UPPER_BOUND
                ) AS source
                ON target.STREAM = source.STREAM
                WHEN MATCHED THEN UPDATE SET UPPER_BOUND = source.UPPER_BOUND
                WHEN NOT MATCHED THEN INSERT (STREAM, UPPER_BOUND)
                    VALUES (source.STREAM, source.UPPER_BOUND)
            """, (upper.astimezone(timezone.utc).isoformat(),))
            connection.commit()
        except Exception:
            connection.rollback()
            raise
