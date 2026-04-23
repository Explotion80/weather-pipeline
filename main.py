"""
Weather pipeline - entrypoint.

Runs Extract -> Transform -> Load for all cities defined in etl/cities.py.

Usage:
    python main.py

Exit codes:
    0 - all cities loaded successfully
    1 - at least one city failed (details in logs)
"""
from __future__ import annotations

import logging
import sys
from time import perf_counter

from etl.cities import CITIES
from etl.db import get_connection
from etl.extract import fetch_weather
from etl.load import load_city
from etl.transform import transform_all

logger = logging.getLogger(__name__)


def run_pipeline() -> tuple[int, int, dict[str, int]]:
    """
    Execute the full ETL for every city in CITIES.

    Returns:
        (success_count, failure_count, totals)
        where `totals` is a dict aggregating inserted row counts across cities.
    """
    success_count = 0
    failure_count = 0
    totals = {"current": 0, "hourly": 0, "daily": 0}

    conn = get_connection()
    try:
        for city in CITIES:
            try:
                raw = fetch_weather(city)
                transformed = transform_all(raw)
                stats = load_city(conn, transformed)

                for key in totals:
                    totals[key] += stats[key]
                success_count += 1

            except Exception as exc:
                # One bad city shouldn't kill the whole run.
                logger.exception("Failed to process %s: %s", city["name"], exc)
                failure_count += 1

                # After an exception the psycopg3 connection is in an error state
                # for the current transaction. Rollback so the next iteration
                # can use a clean transaction.
                try:
                    conn.rollback()
                except Exception:
                    pass
    finally:
        conn.close()

    return success_count, failure_count, totals


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    start = perf_counter()
    logger.info("Starting weather pipeline for %d cities", len(CITIES))

    success, failure, totals = run_pipeline()

    duration = perf_counter() - start

    # Final summary
    print("\n" + "=" * 60)
    print(f"Pipeline finished in {duration:.1f}s")
    print(f"  Cities OK:     {success}/{len(CITIES)}")
    print(f"  Cities failed: {failure}")
    print(f"  Rows inserted: current={totals['current']}, "
          f"hourly={totals['hourly']}, daily={totals['daily']}")
    print("=" * 60)

    # Exit code: 1 if anything failed (useful for cron/CI)
    return 0 if failure == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
