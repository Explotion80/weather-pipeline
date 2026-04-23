"""
Integration tests for etl/load.py

Uses testcontainers to spin up an ephemeral PostgreSQL 16.
Fixtures in conftest.py handle container lifecycle + schema + TRUNCATE between tests.

Run only these tests:
    python -m pytest -m integration

Skip these tests (e.g. when Docker is offline):
    python -m pytest -m "not integration"
"""
from __future__ import annotations

from datetime import date, datetime, time

import pytest

from etl.load import (
    insert_current,
    insert_daily,
    insert_hourly,
    load_city,
    upsert_location,
)

pytestmark = pytest.mark.integration  # apply to every test in this file


# Small synthetic location for tests that don't need real Warsaw data
SYNTHETIC_LOCATION = {
    "name": "TestCity",
    "latitude": 50.0,
    "longitude": 20.0,
    "timezone": "Europe/Warsaw",
    "elevation": 100,
}


def _count_rows(conn, table: str) -> int:
    """Small helper - saves a few lines per test."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


# ==========================================================================
# upsert_location
# ==========================================================================

class TestUpsertLocation:
    def test_inserts_new_location_and_returns_id(self, db_conn):
        loc_id = upsert_location(db_conn, SYNTHETIC_LOCATION)
        db_conn.commit()

        assert isinstance(loc_id, int)
        assert loc_id > 0
        assert _count_rows(db_conn, "locations") == 1

    def test_returns_same_id_on_duplicate_coords(self, db_conn):
        """Idempotency: calling twice with same lat/lon returns same id."""
        id1 = upsert_location(db_conn, SYNTHETIC_LOCATION)
        db_conn.commit()
        id2 = upsert_location(db_conn, SYNTHETIC_LOCATION)
        db_conn.commit()

        assert id1 == id2
        assert _count_rows(db_conn, "locations") == 1  # no duplicate row

    def test_updates_name_when_same_coords_different_name(self, db_conn):
        """ON CONFLICT DO UPDATE: newer name wins, id stays stable."""
        id1 = upsert_location(db_conn, SYNTHETIC_LOCATION)
        renamed = {**SYNTHETIC_LOCATION, "name": "RenamedCity"}
        id2 = upsert_location(db_conn, renamed)
        db_conn.commit()

        assert id1 == id2
        with db_conn.cursor() as cur:
            cur.execute("SELECT name FROM locations WHERE id = %s", (id1,))
            assert cur.fetchone()[0] == "RenamedCity"


# ==========================================================================
# insert_current
# ==========================================================================

class TestInsertCurrent:
    def test_persists_row_with_correct_values(self, db_conn):
        loc_id = upsert_location(db_conn, SYNTHETIC_LOCATION)
        fetched_at = datetime(2026, 4, 16, 12, 0)
        current_row = {
            "recorded_at":        datetime(2026, 4, 16, 11, 30),
            "temperature_c":      15.5,
            "apparent_temp_c":    14.0,
            "humidity_pct":       65,
            "precipitation_mm":   0.0,
            "rain_mm":            0.0,
            "snowfall_cm":        0.0,
            "weather_code":       1,
            "cloud_cover_pct":    30,
            "pressure_hpa":       1015.2,
            "wind_speed_kmh":     12.5,
            "wind_direction_deg": 180,
            "wind_gusts_kmh":     25.0,
            "is_day":             True,
        }
        insert_current(db_conn, loc_id, current_row, fetched_at)
        db_conn.commit()

        # Read back and verify a few key fields
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT temperature_c, humidity_pct, is_day FROM current_weather "
                "WHERE location_id = %s",
                (loc_id,),
            )
            row = cur.fetchone()
        assert float(row[0]) == 15.5
        assert row[1] == 65
        assert row[2] is True


# ==========================================================================
# insert_hourly / insert_daily - bulk inserts
# ==========================================================================

class TestBulkInserts:
    def test_insert_hourly_stores_all_rows(self, db_conn, sample_transformed):
        """Feed transformed data (168 rows) into the bulk insert."""
        loc_id = upsert_location(db_conn, sample_transformed["location"])
        fetched_at = datetime.now()
        hourly_rows = sample_transformed["hourly"]

        inserted = insert_hourly(db_conn, loc_id, hourly_rows, fetched_at)
        db_conn.commit()

        assert inserted == len(hourly_rows)
        assert _count_rows(db_conn, "hourly_forecast") == len(hourly_rows)

    def test_insert_daily_stores_sunrise_sunset_correctly(self, db_conn, sample_transformed):
        loc_id = upsert_location(db_conn, sample_transformed["location"])
        fetched_at = datetime.now()
        daily_rows = sample_transformed["daily"]

        inserted = insert_daily(db_conn, loc_id, daily_rows, fetched_at)
        db_conn.commit()

        assert inserted == len(daily_rows)

        # Spot-check: first row's sunrise/sunset read back as TIME
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT forecast_date, sunrise, sunset FROM daily_forecast "
                "WHERE location_id = %s ORDER BY forecast_date LIMIT 1",
                (loc_id,),
            )
            row = cur.fetchone()
        assert isinstance(row[0], date)
        assert isinstance(row[1], time)
        assert isinstance(row[2], time)
        assert row[1] < row[2]  # sunrise before sunset


# ==========================================================================
# load_city - end-to-end orchestration
# ==========================================================================

class TestLoadCity:
    def test_populates_all_four_tables(self, db_conn, sample_transformed):
        stats = load_city(db_conn, sample_transformed)

        assert stats["current"] == 1
        assert stats["hourly"] == len(sample_transformed["hourly"])
        assert stats["daily"] == len(sample_transformed["daily"])

        assert _count_rows(db_conn, "locations") == 1
        assert _count_rows(db_conn, "current_weather") == 1
        assert _count_rows(db_conn, "hourly_forecast") == len(sample_transformed["hourly"])
        assert _count_rows(db_conn, "daily_forecast") == len(sample_transformed["daily"])

    def test_second_run_creates_new_snapshot_not_duplicate(self, db_conn, sample_transformed):
        """
        Running twice should create TWO snapshots (different fetched_at),
        not fail on unique constraint. That's the whole point of including
        fetched_at in the unique key - we keep historical forecasts.
        """
        load_city(db_conn, sample_transformed)
        first_hourly_count = _count_rows(db_conn, "hourly_forecast")

        load_city(db_conn, sample_transformed)
        second_hourly_count = _count_rows(db_conn, "hourly_forecast")

        # Should have doubled (both snapshots coexist)
        assert second_hourly_count == 2 * first_hourly_count
        # Location still just one row (upsert is idempotent)
        assert _count_rows(db_conn, "locations") == 1

    def test_rolls_back_on_error_inside_transaction(self, db_conn, sample_transformed):
        """
        If anything inside load_city() fails, nothing should be committed.
        We sabotage the input to force a constraint violation mid-transaction.
        """
        bad_data = {
            "location": sample_transformed["location"],
            # Invalid current: recorded_at is required (NOT NULL in schema)
            "current":  {**sample_transformed["current"], "recorded_at": None},
            "hourly":   sample_transformed["hourly"],
            "daily":    sample_transformed["daily"],
        }

        with pytest.raises(Exception):  # psycopg.errors.NotNullViolation
            load_city(db_conn, bad_data)

        # Transaction rolled back - NOTHING persisted, not even the location
        assert _count_rows(db_conn, "locations") == 0
        assert _count_rows(db_conn, "current_weather") == 0
        assert _count_rows(db_conn, "hourly_forecast") == 0
        assert _count_rows(db_conn, "daily_forecast") == 0
