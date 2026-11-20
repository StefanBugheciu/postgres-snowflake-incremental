import argparse
import logging

import sink
import snow
import source

LOCAL_ACTIONS = {
    "init-source": "postgres_setup.sql",
    "seed": "postgres_seed.sql",
    "change": "postgres_change.sql",
}
RUN_LOCK = 764832101


def sync(postgres, snowflake) -> dict[str, int]:
    # Session lock serializes runs against the same PostgreSQL source.
    acquired = postgres.execute("SELECT pg_try_advisory_lock(%s)", (RUN_LOCK,)).fetchone()[0]
    if not acquired:
        raise RuntimeError("Another sync is running")
    try:
        lower = sink.get_watermark(snowflake)
        upper, batch = source.read_window(postgres, lower)
        counts = {table: len(rows) for table, rows in batch.items()}
        logging.info("%s extraction before %s: %s",
                     "Initial" if lower is None else "Incremental", upper, counts)
        sink.load_window(snowflake, upper, batch)
        logging.info("Committed rows and watermark %s", upper)
        return counts
    finally:
        postgres.execute("SELECT pg_advisory_unlock(%s)", (RUN_LOCK,))


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv(snow.ROOT / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=(*LOCAL_ACTIONS, "init-target", "sync", "verify"))
    action = parser.parse_args().action
    if action in LOCAL_ACTIONS:
        with source.connect() as postgres:
            source.run_script(postgres, LOCAL_ACTIONS[action])
        logging.info("Completed %s", action)
    elif action == "sync":
        with source.connect() as postgres, snow.connect() as snowflake:
            sync(postgres, snowflake)
    else:
        with snow.connect() as snowflake:
            snow.run_sql(snowflake, "setup.sql" if action == "init-target" else "verify.sql",
                         show_results=action == "verify")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        main()
    except Exception as exc:
        detail = str(exc) if isinstance(exc, (ValueError, RuntimeError)) else (
            "Check database connectivity, configuration, permissions and initialization."
        )
        logging.error("Incremental pipeline failed (%s). %s", type(exc).__name__, detail)
        raise SystemExit(1) from None
