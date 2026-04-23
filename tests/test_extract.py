"""
Unit tests for etl/extract.py

Uses the `responses` library to mock HTTP calls - no real network traffic.
All tests are fast because we also mock `time.sleep` (via autouse fixture)
so retry delays don't actually pause the test suite.
"""
from __future__ import annotations

import pytest
import requests
import responses

from etl.cities import CITIES
from etl.extract import API_URL, MAX_RETRIES, fetch_all, fetch_weather

# Reusable test city - we don't need every test to build one by hand
WARSAW = {"name": "Warsaw", "latitude": 52.23, "longitude": 21.01}


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """
    Autouse: replace time.sleep with a no-op for every test in this file.

    Without this, retry tests would pause RETRY_DELAY seconds per attempt -
    turning a 50ms test into a 2-second one. The real sleep has no test value.
    """
    monkeypatch.setattr("etl.extract.time.sleep", lambda *a, **kw: None)


# ==========================================================================
# fetch_weather - happy path and request shape
# ==========================================================================

class TestFetchWeatherHappyPath:
    @responses.activate
    def test_returns_parsed_json(self):
        responses.add(
            responses.GET, API_URL,
            json={"latitude": 52.23, "longitude": 21.01, "current": {"temp": 10}},
            status=200,
        )
        result = fetch_weather(WARSAW)
        assert result["latitude"] == 52.23
        assert result["current"]["temp"] == 10

    @responses.activate
    def test_attaches_city_name_to_response(self):
        # API doesn't know our city's friendly name - extract.py adds it
        responses.add(responses.GET, API_URL, json={"latitude": 52.23}, status=200)
        result = fetch_weather(WARSAW)
        assert result["city_name"] == "Warsaw"

    @responses.activate
    def test_sends_correct_query_params(self):
        responses.add(responses.GET, API_URL, json={}, status=200)
        fetch_weather(WARSAW)

        # Inspect the actual URL that was requested
        called_url = responses.calls[0].request.url
        assert "latitude=52.23" in called_url
        assert "longitude=21.01" in called_url
        assert "current=" in called_url       # field list present
        assert "hourly=" in called_url
        assert "daily=" in called_url
        assert "timezone=auto" in called_url


# ==========================================================================
# fetch_weather - retry behavior on transient errors
# ==========================================================================

class TestFetchWeatherRetry:
    @responses.activate
    def test_retries_on_timeout_then_succeeds(self):
        # First call: timeout. Second call: 200 OK.
        responses.add(responses.GET, API_URL, body=requests.Timeout("slow"))
        responses.add(responses.GET, API_URL, json={"ok": True}, status=200)

        result = fetch_weather(WARSAW)
        assert result["ok"] is True
        assert len(responses.calls) == 2  # 1 failed + 1 succeeded

    @responses.activate
    def test_retries_on_connection_error_then_succeeds(self):
        responses.add(responses.GET, API_URL, body=requests.ConnectionError("no net"))
        responses.add(responses.GET, API_URL, json={"ok": True}, status=200)

        result = fetch_weather(WARSAW)
        assert result["ok"] is True
        assert len(responses.calls) == 2

    @responses.activate
    def test_raises_after_all_retries_exhausted(self):
        # Register MAX_RETRIES (2) timeouts - every attempt fails
        for _ in range(MAX_RETRIES):
            responses.add(responses.GET, API_URL, body=requests.Timeout("always slow"))

        with pytest.raises(requests.Timeout):
            fetch_weather(WARSAW)
        assert len(responses.calls) == MAX_RETRIES


# ==========================================================================
# fetch_weather - HTTP errors should NOT trigger retries
# ==========================================================================

class TestFetchWeatherNoRetryOnHttpError:
    @responses.activate
    def test_http_500_raises_immediately_without_retry(self):
        # 5xx is not considered transient - fail fast
        responses.add(responses.GET, API_URL, json={"error": "server"}, status=500)

        with pytest.raises(requests.HTTPError):
            fetch_weather(WARSAW)
        assert len(responses.calls) == 1  # no retry happened

    @responses.activate
    def test_http_400_raises_immediately_without_retry(self):
        # 4xx is a client error - retrying won't help
        responses.add(responses.GET, API_URL, json={"error": "bad params"}, status=400)

        with pytest.raises(requests.HTTPError):
            fetch_weather(WARSAW)
        assert len(responses.calls) == 1


# ==========================================================================
# fetch_all - orchestrator over all cities, with per-city error isolation
# ==========================================================================

class TestFetchAll:
    @responses.activate
    def test_fetches_weather_for_all_cities(self):
        # One success per city (responses queue matches in FIFO order)
        for city in CITIES:
            responses.add(
                responses.GET, API_URL,
                json={"latitude": city["latitude"], "longitude": city["longitude"]},
                status=200,
            )

        results = fetch_all()
        assert len(results) == len(CITIES)
        # Each result should have city_name attached, in the right order
        assert [r["city_name"] for r in results] == [c["name"] for c in CITIES]

    @responses.activate
    def test_continues_when_one_city_fails(self):
        # First city fails with 500, the rest succeed
        responses.add(responses.GET, API_URL, json={"error": "down"}, status=500)
        for _ in CITIES[1:]:
            responses.add(responses.GET, API_URL, json={}, status=200)

        results = fetch_all()
        # 5 cities - 1 failed = 4 results
        assert len(results) == len(CITIES) - 1
        # The surviving cities should be cities 2..5 (skipped the first)
        surviving_names = [r["city_name"] for r in results]
        assert surviving_names == [c["name"] for c in CITIES[1:]]
