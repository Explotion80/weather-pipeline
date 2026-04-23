"""
Unit tests for etl/transform.py

Tests the column-to-row pivot logic and field mapping.

Mix of two styles:
  - Synthetic dicts for testing logic branches (is_day conversion, None handling)
  - Real fixture (Warsaw API response) for testing end-to-end shape
"""
from __future__ import annotations

from datetime import date, datetime, time

from etl.transform import (
    _parse_date,
    _parse_datetime,
    _parse_time_from_iso,
    transform_all,
    transform_current,
    transform_daily,
    transform_hourly,
    transform_location,
)


# ==========================================================================
# Parser helpers - all must be None-safe (API may return nulls)
# ==========================================================================

class TestParsers:
    def test_parse_datetime_valid_iso(self):
        assert _parse_datetime("2026-04-15T14:30") == datetime(2026, 4, 15, 14, 30)

    def test_parse_datetime_none_returns_none(self):
        assert _parse_datetime(None) is None

    def test_parse_date_valid(self):
        assert _parse_date("2026-04-15") == date(2026, 4, 15)

    def test_parse_date_none_returns_none(self):
        assert _parse_date(None) is None

    def test_parse_time_from_iso_extracts_time_only(self):
        # Input is full datetime, output should be just the time part
        assert _parse_time_from_iso("2026-04-15T05:45") == time(5, 45)

    def test_parse_time_from_iso_none_returns_none(self):
        assert _parse_time_from_iso(None) is None


# ==========================================================================
# transform_location - extracts the single `locations` row
# ==========================================================================

class TestTransformLocation:
    def test_extracts_all_fields_from_fixture(self, sample_api_response):
        result = transform_location(sample_api_response)
        assert result["name"] == "Warsaw"
        assert isinstance(result["latitude"], (int, float))
        assert isinstance(result["longitude"], (int, float))
        # Warsaw is ~52N 21E
        assert 51 < result["latitude"] < 53
        assert 20 < result["longitude"] < 22

    def test_missing_optionals_become_none(self):
        raw = {"city_name": "TestCity", "latitude": 50.0, "longitude": 20.0}
        result = transform_location(raw)
        assert result["name"] == "TestCity"
        assert result["timezone"] is None
        assert result["elevation"] is None


# ==========================================================================
# transform_current - single `current_weather` row + is_day conversion
# ==========================================================================

class TestTransformCurrent:
    def test_maps_api_fields_to_db_column_names(self, sample_api_response):
        result = transform_current(sample_api_response)
        # After renaming: DB names present, original API names absent
        assert "temperature_c" in result
        assert "temperature_2m" not in result
        assert "humidity_pct" in result
        assert "relative_humidity_2m" not in result
        assert "wind_speed_kmh" in result

    def test_recorded_at_parsed_as_datetime(self, sample_api_response):
        result = transform_current(sample_api_response)
        assert isinstance(result["recorded_at"], datetime)

    def test_is_day_one_converts_to_true(self):
        raw = {"current": {"time": "2026-04-15T12:00", "is_day": 1}}
        result = transform_current(raw)
        assert result["is_day"] is True

    def test_is_day_zero_converts_to_false(self):
        raw = {"current": {"time": "2026-04-15T22:00", "is_day": 0}}
        result = transform_current(raw)
        assert result["is_day"] is False

    def test_is_day_missing_stays_none(self):
        raw = {"current": {"time": "2026-04-15T12:00"}}  # no is_day key
        result = transform_current(raw)
        assert result["is_day"] is None


# ==========================================================================
# transform_hourly - pivot parallel arrays into list of row-dicts
# ==========================================================================

class TestTransformHourly:
    def test_pivots_all_rows_from_fixture(self, sample_api_response):
        result = transform_hourly(sample_api_response)
        # One output row per timestamp in input
        assert len(result) == len(sample_api_response["hourly"]["time"])
        assert len(result) > 0  # sanity: fixture isn't empty

    def test_forecast_time_is_datetime_type(self, sample_api_response):
        result = transform_hourly(sample_api_response)
        assert isinstance(result[0]["forecast_time"], datetime)

    def test_first_row_values_align_with_source_arrays(self, sample_api_response):
        """Position i in output = position i in every input array."""
        result = transform_hourly(sample_api_response)
        expected_temp = sample_api_response["hourly"]["temperature_2m"][0]
        expected_wind = sample_api_response["hourly"]["wind_speed_10m"][0]
        assert result[0]["temperature_c"] == expected_temp
        assert result[0]["wind_speed_kmh"] == expected_wind

    def test_missing_hourly_key_returns_empty_list(self):
        # Defensive: if API response is malformed, don't crash
        assert transform_hourly({}) == []
        assert transform_hourly({"other": "data"}) == []

    def test_all_rows_have_field_from_mapping(self, sample_api_response):
        result = transform_hourly(sample_api_response)
        # Every row must contain every mapped DB column
        expected_keys = {
            "forecast_time", "temperature_c", "humidity_pct",
            "precipitation_mm", "wind_speed_kmh",
        }
        assert expected_keys.issubset(result[0].keys())


# ==========================================================================
# transform_daily - daily rows + sunrise/sunset TIME conversion
# ==========================================================================

class TestTransformDaily:
    def test_pivots_all_daily_rows(self, sample_api_response):
        result = transform_daily(sample_api_response)
        assert len(result) == len(sample_api_response["daily"]["time"])

    def test_forecast_date_is_date_not_datetime(self, sample_api_response):
        result = transform_daily(sample_api_response)
        # Must be pure date (not datetime, which is a subclass of date)
        assert type(result[0]["forecast_date"]) is date

    def test_sunrise_sunset_extracted_as_time_type(self, sample_api_response):
        result = transform_daily(sample_api_response)
        assert isinstance(result[0]["sunrise"], time)
        assert isinstance(result[0]["sunset"], time)
        # Sanity: sunrise before sunset (any day anywhere on Earth, except polar regions)
        assert result[0]["sunrise"] < result[0]["sunset"]


# ==========================================================================
# transform_all - orchestrator returning 4 buckets
# ==========================================================================

class TestTransformAll:
    def test_returns_all_four_buckets(self, sample_api_response):
        result = transform_all(sample_api_response)
        assert set(result.keys()) == {"location", "current", "hourly", "daily"}

    def test_bucket_types_are_correct(self, sample_api_response):
        result = transform_all(sample_api_response)
        assert isinstance(result["location"], dict)
        assert isinstance(result["current"], dict)
        assert isinstance(result["hourly"], list)
        assert isinstance(result["daily"], list)
