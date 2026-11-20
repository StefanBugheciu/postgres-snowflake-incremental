import os
from datetime import datetime
from pathlib import Path

TABLES = {
    "customers": ("id", "name", "city", "updated_at"),
    "orders": ("id", "customer_id", "amount", "status", "updated_at"),
}
ROOT = Path(__file__).resolve().parent


def connect():
    import psycopg

    return psycopg.connect(
        host=os.getenv("PGHOST", "127.0.0.1"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "incremental_demo"),
        user=os.getenv("PGUSER", "demo"),
        connect_timeout=5,
        autocommit=True,
    )


def run_script(connection, filename: str) -> None:
    sql = (ROOT / "sql" / filename).read_text(encoding="utf-8")
    with connection.transaction():
        connection.execute(sql)


def read_window(connection, lower: datetime | None) -> tuple[datetime, dict[str, list[tuple]]]:
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        connection.execute("SET LOCAL lock_timeout = '5s'")
        # Wait for active writes and hold new writes until extraction finishes.
        connection.execute("LOCK TABLE public.customers, public.orders IN SHARE MODE")
        upper = connection.execute("SELECT clock_timestamp()").fetchone()[0]
        if lower is not None and upper <= lower:
            raise ValueError("PostgreSQL clock did not advance beyond the saved watermark")

        batch = {}
        for table, columns in TABLES.items():
            query = f"SELECT {', '.join(columns)} FROM public.{table} WHERE updated_at < %s"
            params = [upper]
            if lower is not None:
                query += " AND updated_at >= %s"
                params.append(lower)
            query += " ORDER BY updated_at, id"
            batch[table] = connection.execute(query, params).fetchall()
    return upper, batch
