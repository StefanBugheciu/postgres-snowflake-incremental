import os

import pytest

import source


@pytest.mark.skipif(os.getenv("RUN_POSTGRES_INTEGRATION") != "1",
                    reason="Requires an empty dedicated PostgreSQL demo database")
def test_full_incremental_and_noop_windows():
    with source.connect() as pg:
        tables = pg.execute(
            "SELECT to_regclass('public.customers'), to_regclass('public.orders')"
        ).fetchone()
        assert tables == (None, None), "Use an empty dedicated demo database"
        source.run_script(pg, "postgres_setup.sql")
        source.run_script(pg, "postgres_seed.sql")
        first, batch = source.read_window(pg, None)
        assert [row[0] for row in batch["customers"]] == [1, 2, 3]
        assert [row[0] for row in batch["orders"]] == [101, 102, 103, 104]

        source.run_script(pg, "postgres_change.sql")
        second, batch = source.read_window(pg, first)
        assert [row[0] for row in batch["customers"]] == [1, 4]
        assert batch["customers"][0][2] == "Brasov"
        tied_timestamp = batch["customers"][0][-1]
        assert tied_timestamp == batch["customers"][1][-1]
        assert batch["orders"] == []

        _, at_boundary = source.read_window(pg, tied_timestamp)
        assert at_boundary == batch

        source.run_script(pg, "postgres_change.sql")
        _, batch = source.read_window(pg, second)
        assert batch == {"customers": [], "orders": []}
