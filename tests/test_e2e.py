"""
End-to-end tests for the full ETL pipeline.

Tests main.run_pipeline() against:
  - mocked HTTP (responses library) - no real API calls
  - real PostgreSQL (testcontainers) - real SQL, real transactions

This is the 'smoke test' tier - doesn't dive deep into any one module,
but proves all three modules (extract / transform / load) glue together.
"""
from __future__ import annotations

import psycopg
import pytest
import responses

import main
from etl.cities import CITIES
from etl.extract import API_URL

pytestmark = pytest.mark.integration  # all tests here need Docker


def _count_rows(conn, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    """Don't actually sleep between retries (would slow the test on failures)."""
    monkeypatch.setattr("etl.extract.time.sleep", lambda *a, **kw: None)


@pytest.fixture
def patched_get_connection(db_connection_params, monkeypatch):
    """
    Redirect main.get_connection() to our testcontainer.

    IMPORTANT: we patch 'main.get_connection', not 'etl.db.get_connection',
    because main.py did `from etl.db import get_connection` - that created
    a separate binding in main's namespace.
    """
    def factory():
        # Fresh connection per call - matches real get_connection() behavior
        return psycopg.connect(**db_connection_params)

    monkeypatch.setattr("main.get_connection", factory)


# ==========================================================================
# Happy path - every city succeeds
# ==========================================================================

class TestFullPipeline:
    @responses.activate
    def test_happy_path_all_cities_succeed(
        self, db_conn, patched_get_connection, sample_api_response
    ):
        # Strip city_name - real API doesn't return it (extract.py adds it)
        raw_api = {k: v for k, v in sample_api_response.items() if k != "city_name"}

        # Register one success response per city (FIFO queue)
        for _ in CITIES:
            responses.add(responses.GET, API_URL, json=raw_api, status=200)

        # Act: run the whole pipeline
        success, failure, totals = main.run_pipeline()

        # Assert: return values
        assert success == len(CITIES)
        assert failure == 0
        assert totals["current"] == len(CITIES)
        assert totals["hourly"] == len(CITIES) * len(sample_api_response["hourly"]["time"])
        assert totals["daily"] == len(CITIES) * len(sample_api_response["daily"]["time"])

        # Assert: DB actually has the data
        assert _count_rows(db_conn, "locations") == len(CITIES)
        assert _count_rows(db_conn, "current_weather") == len(CITIES)
        assert _count_rows(db_conn, "hourly_forecast") == totals["hourly"]
        assert _count_rows(db_conn, "daily_forecast") == totals["daily"]

        # Assert: HTTP was called exactly once per city
        assert len(responses.calls) == len(CITIES)


# ==========================================================================
# Fault isolation - one failing city must not kill the whole run
# ==========================================================================

class TestFailureIsolation:
    @responses.activate
    def test_pipeline_continues_when_one_city_api_returns_500(
        self, db_conn, patched_get_connection, sample_api_response
    ):
        raw_api = {k: v for k, v in sample_api_response.items() if k != "city_name"}

        # First city gets 500, rest succeed
        responses.add(responses.GET, API_URL, json={"error": "down"}, status=500)
        for _ in CITIES[1:]:
            responses.add(responses.GET, API_URL, json=raw_api, status=200)

        success, failure, totals = main.run_pipeline()

        # One failure, rest OK
        assert failure == 1
        assert success == len(CITIES) - 1

        # DB has data only from the 4 successful cities
        assert _count_rows(db_conn, "locations") == len(CITIES) - 1
        assert _count_rows(db_conn, "current_weather") == len(CITIES) - 1

        # The failed city's work was rolled back - no partial state
        # (would be caught if rollback in main.py was missing)
        hourly_per_city = len(sample_api_response["hourly"]["time"])
        assert _count_rows(db_conn, "hourly_forecast") == (len(CITIES) - 1) * hourly_per_city
