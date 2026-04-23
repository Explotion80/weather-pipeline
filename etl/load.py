"""
Load module - writes transformed weather data into PostgreSQL.

Flow per city:
    1. upsert_location() - get or create the location row, return its id
    2. insert_current()  - single row for current_weather
    3. insert_hourly()   - bulk insert ~168 rows for hourly_forecast
    4. insert_daily()    - bulk insert ~7 rows for daily_forecast

All inserts for one city happen in a single transaction (via `with conn`).
If anything fails, nothing gets committed for that city.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Explicit column lists - order matters for bulk inserts below.
# Kept as constants so the SQL and the Python dict lookups stay in sync.
# -------------------------------------------------------------------

CURRENT_COLUMNS = [
    "location_id", "recorded_at",
    "temperature_c", "apparent_temp_c", "humidity_pct",
    "precipitation_mm", "rain_mm", "snowfall_cm",
    "weather_code", "cloud_cover_pct", "pressure_hpa",
    "wind_speed_kmh", "wind_direction_deg", "wind_gusts_kmh",
    "is_day", "fetched_at",
]

HOURLY_COLUMNS = [
    "location_id", "forecast_time",
    "temperature_c", "apparent_temp_c", "humidity_pct", "dew_point_c",
    "precipitation_mm", "precipitation_probability",
    "rain_mm", "snowfall_cm",
    "weather_code", "cloud_cover_pct", "visibility_m",
    "wind_speed_kmh", "wind_direction_deg", "wind_gusts_kmh",
    "fetched_at",
]

DAILY_COLUMNS = [
    "location_id", "forecast_date",
    "temperature_max_c", "temperature_min_c", "temperature_mean_c",
    "precipitation_sum_mm", "precipitation_probability", "precipitation_hours",
    "rain_sum_mm", "snowfall_sum_cm",
    "weather_code", "sunrise", "sunset", "sunshine_duration_s",
    "wind_speed_max_kmh", "wind_gusts_max_kmh", "wind_direction_dominant_deg",
    "uv_index_max",
    "fetched_at",
]


# -------------------------------------------------------------------
# Individual loaders - one per table
# -------------------------------------------------------------------

def upsert_location(conn, location: dict[str, Any]) -> int:
    """
    Ensure the location row exists and return its id.

    Atomic upsert: INSERT and let ON CONFLICT handle the "already exists" case.
    DO UPDATE (not DO NOTHING) so that RETURNING id works for both paths.
    Safe under float-vs-DECIMAL precision quirks - Postgres uses the stored
    DECIMAL index for the conflict check, not float equality.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO locations (name, latitude, longitude, timezone, elevation)
            VALUES (%(name)s, %(latitude)s, %(longitude)s, %(timezone)s, %(elevation)s)
            ON CONFLICT (latitude, longitude) DO UPDATE
              SET name = EXCLUDED.name  -- no-op update so RETURNING works
            RETURNING id
            """,
            location,
        )
        return cur.fetchone()[0]


def insert_current(conn, location_id: int, row: dict[str, Any], fetched_at: datetime) -> None:
    """Insert one row into current_weather."""
    row = {**row, "location_id": location_id, "fetched_at": fetched_at}
    values = tuple(row.get(col) for col in CURRENT_COLUMNS)

    placeholders = ", ".join(["%s"] * len(CURRENT_COLUMNS))
    columns_sql = ", ".join(CURRENT_COLUMNS)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO current_weather ({columns_sql}) VALUES ({placeholders})",
            values,
        )


def _bulk_insert(conn, table: str, columns: list[str],
                 rows: list[dict[str, Any]], location_id: int,
                 fetched_at: datetime, conflict_cols: str) -> int:
    """
    Generic bulk insert for hourly/daily tables.

    Uses psycopg3's executemany() - with prepared statements under the hood
    it's efficient enough for our row counts (~168 hourly, ~7 daily per city).

    Returns the number of rows actually inserted (ON CONFLICT DO NOTHING may skip some).
    """
    if not rows:
        return 0

    # Materialize as tuples in the exact column order
    tuples = [
        tuple(
            {**r, "location_id": location_id, "fetched_at": fetched_at}.get(col)
            for col in columns
        )
        for r in rows
    ]

    columns_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = (
        f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_cols}) DO NOTHING"
    )

    with conn.cursor() as cur:
        cur.executemany(sql, tuples)
        return cur.rowcount


def insert_hourly(conn, location_id: int, rows: list[dict[str, Any]],
                  fetched_at: datetime) -> int:
    """Bulk insert hourly forecast rows."""
    return _bulk_insert(
        conn, "hourly_forecast", HOURLY_COLUMNS, rows,
        location_id, fetched_at,
        conflict_cols="location_id, forecast_time, fetched_at",
    )


def insert_daily(conn, location_id: int, rows: list[dict[str, Any]],
                 fetched_at: datetime) -> int:
    """Bulk insert daily forecast rows."""
    return _bulk_insert(
        conn, "daily_forecast", DAILY_COLUMNS, rows,
        location_id, fetched_at,
        conflict_cols="location_id, forecast_date, fetched_at",
    )


# -------------------------------------------------------------------
# Orchestration: load one city end-to-end
# -------------------------------------------------------------------

def load_city(conn, transformed: dict[str, Any]) -> dict[str, int]:
    """
    Load all 4 buckets (location/current/hourly/daily) for one city.

    Uses one fetched_at timestamp for the whole batch so everything
    from a single run is consistent. Runs inside a transaction:
    on error, nothing for this city is committed.

    Returns a stats dict with row counts.
    """
    fetched_at = datetime.now()
    stats = {"current": 0, "hourly": 0, "daily": 0}

    # psycopg3: conn.transaction() manages a transaction without closing the conn.
    # (Note: `with conn:` in psycopg3 CLOSES the connection on exit - don't use it here.)
    with conn.transaction():
        location_id = upsert_location(conn, transformed["location"])

        insert_current(conn, location_id, transformed["current"], fetched_at)
        stats["current"] = 1

        stats["hourly"] = insert_hourly(conn, location_id, transformed["hourly"], fetched_at)
        stats["daily"]  = insert_daily(conn, location_id, transformed["daily"], fetched_at)

    logger.info(
        "Loaded %s: current=%d, hourly=%d, daily=%d",
        transformed["location"]["name"],
        stats["current"], stats["hourly"], stats["daily"],
    )
    return stats


# -------------------------------------------------------------------
# Standalone test: python -m etl.load
# -------------------------------------------------------------------

if __name__ == "__main__":
    from etl.cities import CITIES
    from etl.db import get_connection
    from etl.extract import fetch_weather
    from etl.transform import transform_all

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Test with Warsaw
    raw = fetch_weather(CITIES[0])
    transformed = transform_all(raw)

    conn = get_connection()
    try:
        stats = load_city(conn, transformed)
        print(f"\nLoaded stats: {stats}")

        # Verify via COUNT(*)
        with conn.cursor() as cur:
            for table in ("locations", "current_weather", "hourly_forecast", "daily_forecast"):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  {table:20s}: {count} rows")
    finally:
        conn.close()
