from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import main
import sink
import source

LOWER = datetime(2026, 1, 1, tzinfo=timezone.utc)
UPPER = LOWER + timedelta(seconds=1)
CUSTOMERS = [(1, "Ana Demo", "Brasov", LOWER), (4, "Vlad Demo", "Timisoara", LOWER)]


@pytest.mark.parametrize("lower", [None, LOWER], ids=["full", "incremental"])
def test_extracts_half_open_window_in_stable_order(lower):
    pg = MagicMock()
    pg.execute.return_value.fetchone.return_value = (UPPER,)
    pg.execute.return_value.fetchall.side_effect = [CUSTOMERS, []]
    assert source.read_window(pg, lower) == (UPPER, {"customers": CUSTOMERS, "orders": []})
    sql, params = pg.execute.call_args_list[-2].args
    assert "updated_at < %s" in sql
    assert "ORDER BY updated_at, id" in sql
    assert ("updated_at >= %s" in sql) == (lower is not None)
    assert params == ([UPPER, LOWER] if lower else [UPPER])


def test_rejects_backward_source_clock():
    pg = MagicMock()
    pg.execute.return_value.fetchone.return_value = (LOWER,)
    with pytest.raises(ValueError, match="clock did not advance"):
        source.read_window(pg, LOWER)
    pg.execute.return_value.fetchall.assert_not_called()


def test_watermark_updated_after_both_merges(monkeypatch):
    conn = MagicMock()
    stage = MagicMock()
    monkeypatch.setattr(sink.snow, "run_sql", stage)
    cursor = conn.cursor.return_value.__enter__.return_value
    sink.load_window(conn, UPPER, {
        "customers": CUSTOMERS,
        "orders": [(101, 1, Decimal("45.90"), "paid", LOWER)],
    })
    stage.assert_called_once_with(conn, "staging.sql")
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    inserts = [call for call in cursor.execute.call_args_list if call.args[0].startswith("INSERT")]
    assert len(inserts) == 3
    assert inserts[0].args[1] == (1, "Ana Demo", "Brasov", LOWER.isoformat())
    assert inserts[-1].args[1] == (101, 1, Decimal("45.90"), "paid", LOWER.isoformat())
    assert statements[0] == "BEGIN"
    assert "MERGE INTO INCREMENTAL_PIPELINE_DB.ANALYTICS.CUSTOMERS" in " ".join(statements)
    assert "MERGE INTO INCREMENTAL_PIPELINE_DB.ANALYTICS.ORDERS" in " ".join(statements)
    assert "MERGE INTO INCREMENTAL_PIPELINE_DB.CONTROL.WATERMARK" in statements[-1]
    conn.commit.assert_called_once()


def test_failed_merge_rolls_back_without_advancing_watermark(monkeypatch):
    conn = MagicMock()
    monkeypatch.setattr(sink.snow, "run_sql", MagicMock())
    cursor = conn.cursor.return_value.__enter__.return_value

    def execute(sql, *args):
        if ".ANALYTICS.CUSTOMERS" in sql:
            raise RuntimeError("merge failed")

    cursor.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="merge failed"):
        sink.load_window(conn, UPPER, {"customers": CUSTOMERS, "orders": []})
    assert all(".CONTROL.WATERMARK" not in call.args[0] for call in cursor.execute.call_args_list)
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_empty_batch_updates_only_watermark(monkeypatch):
    conn = MagicMock()
    stage = MagicMock()
    monkeypatch.setattr(sink.snow, "run_sql", stage)
    sink.load_window(conn, UPPER, {"customers": [], "orders": []})
    cursor = conn.cursor.return_value.__enter__.return_value
    assert cursor.execute.call_count == 2
    assert ".CONTROL.WATERMARK" in cursor.execute.call_args.args[0]
    stage.assert_not_called()
    conn.commit.assert_called_once()


def test_second_sync_is_rejected_if_source_lock_is_held(monkeypatch):
    pg = MagicMock()
    pg.execute.return_value.fetchone.return_value = (False,)
    get_watermark = MagicMock()
    monkeypatch.setattr(sink, "get_watermark", get_watermark)
    with pytest.raises(RuntimeError, match="Another sync is running"):
        main.sync(pg, MagicMock())
    get_watermark.assert_not_called()


def test_sync_releases_source_lock_after_failed_load(monkeypatch):
    pg = MagicMock()
    pg.execute.return_value.fetchone.return_value = (True,)
    monkeypatch.setattr(sink, "get_watermark", lambda conn: LOWER)
    monkeypatch.setattr(source, "read_window", lambda conn, lower: (UPPER, {"customers": [], "orders": []}))
    monkeypatch.setattr(sink, "load_window", MagicMock(side_effect=RuntimeError("load failed")))
    with pytest.raises(RuntimeError, match="load failed"):
        main.sync(pg, MagicMock())
    assert "pg_advisory_unlock" in pg.execute.call_args.args[0]
