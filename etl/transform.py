"""
Transform module - cleans raw Open-Meteo JSON into DB-ready records.

Open-Meteo returns data in a COLUMNAR format (parallel arrays):
    hourly = {
        "time": ["2026-04-15T00:00", "2026-04-15T01:00", ...],
        "temperature_2m": [5.1, 4.8, ...],
        ...
    }

We pivot it into ROWS (list of dicts) matching our DB table columns:
    [
        {"forecast_time": datetime(2026,4,15,0,0), "temperature_c": 5.1, ...},
        {"forecast_time": datetime(2026,4,15,1,0), "temperature_c": 4.8, ...},
    ]
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Any

logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Field mappings: API name  ->  DB column name
# One place to tweak naming without touching transform logic.
# -------------------------------------------------------------------

CURRENT_FIELD_MAP = {
    "temperature_2m":       "temperature_c",
    "apparent_temperature": "apparent_temp_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation":        "precipitation_mm",
    "rain":                 "rain_mm",
    "snowfall":             "snowfall_cm",
    "weather_code":         "weather_code",
    "cloud_cover":          "cloud_cover_pct",
    "pressure_msl":         "pressure_hpa",
    "wind_speed_10m":       "wind_speed_kmh",
    "wind_direction_10m":   "wind_direction_deg",
    "wind_gusts_10m":       "wind_gusts_kmh",
    # is_day handled separately - needs int->bool conversion
}

HOURLY_FIELD_MAP = {
    "temperature_2m":            "temperature_c",
    "apparent_temperature":      "apparent_temp_c",
    "relative_humidity_2m":      "humidity_pct",
    "dew_point_2m":              "dew_point_c",
    "precipitation":             "precipitation_mm",
    "precipitation_probability": "precipitation_probability",
    "rain":                      "rain_mm",
    "snowfall":                  "snowfall_cm",
    "weather_code":              "weather_code",
    "cloud_cover":               "cloud_cover_pct",
    "visibility":                "visibility_m",
    "wind_speed_10m":            "wind_speed_kmh",
    "wind_direction_10m":        "wind_direction_deg",
    "wind_gusts_10m":            "wind_gusts_kmh",
}

DAILY_FIELD_MAP = {
    "temperature_2m_max":            "temperature_max_c",
    "temperature_2m_min":            "temperature_min_c",
    "temperature_2m_mean":           "temperature_mean_c",
    "precipitation_sum":             "precipitation_sum_mm",
    "precipitation_probability_max": "precipitation_probability",
    "precipitation_hours":           "precipitation_hours",
    "rain_sum":                      "rain_sum_mm",
    "snowfall_sum":                  "snowfall_sum_cm",
    "weather_code":                  "weather_code",
    "sunshine_duration":             "sunshine_duration_s",
    "wind_speed_10m_max":            "wind_speed_max_kmh",
    "wind_gusts_10m_max":            "wind_gusts_max_kmh",
    "wind_direction_10m_dominant":   "wind_direction_dominant_deg",
    "uv_index_max":                  "uv_index_max",
    # sunrise/sunset handled separately - need ISO datetime -> TIME conversion
}


# -------------------------------------------------------------------
# Parsing helpers - all tolerant of None (API may return nulls)
# -------------------------------------------------------------------

def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO8601 like '2026-04-15T14:00' into naive datetime."""
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _parse_date(value: str | None) -> date | None:
    """Parse 'YYYY-MM-DD' into date."""
    if value is None:
        return None
    return date.fromisoformat(value)


def _parse_time_from_iso(value: str | None) -> time | None:
    """Extract TIME from full ISO datetime. Used for sunrise/sunset."""
    if value is None:
        return None
    return datetime.fromisoformat(value).time()


# -------------------------------------------------------------------
# Transform functions - one per target DB table
# -------------------------------------------------------------------

def transform_location(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the single `locations` row from the API response."""
    return {
        "name":      raw["city_name"],
        "latitude":  raw["latitude"],
        "longitude": raw["longitude"],
        "timezone":  raw.get("timezone"),
        "elevation": raw.get("elevation"),
    }


def transform_current(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the single `current_weather` row."""
    current = raw.get("current", {})
    row = {"recorded_at": _parse_datetime(current.get("time"))}

    # Map every field via the mapping dict
    for api_name, db_name in CURRENT_FIELD_MAP.items():
        row[db_name] = current.get(api_name)

    # Special: is_day comes as 0/1 int, our DB column is BOOLEAN
    is_day = current.get("is_day")
    row["is_day"] = bool(is_day) if is_day is not None else None

    return row


def _pivot_series(series: dict[str, Any], field_map: dict[str, str],
                  time_key: str, time_parser) -> list[dict[str, Any]]:
    """
    Generic 'columns-to-rows' pivot.

    Takes parallel arrays from API (series) and produces a list of row-dicts.
    Used by both transform_hourly and transform_daily (they share this shape).
    """
    times = series.get(time_key, [])
    rows = []

    for i, t in enumerate(times):
        parsed_time = time_parser(t)
        if parsed_time is None:
            logger.warning("Skipping row with missing time at index %d", i)
            continue

        row: dict[str, Any] = {}
        for api_name, db_name in field_map.items():
            values = series.get(api_name) or []
            row[db_name] = values[i] if i < len(values) else None

        rows.append((parsed_time, row))

    return rows


def transform_hourly(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Pivot `hourly` columnar data into a list of row-dicts."""
    hourly = raw.get("hourly", {})
    pivoted = _pivot_series(hourly, HOURLY_FIELD_MAP, "time", _parse_datetime)

    result = []
    for forecast_time, row in pivoted:
        row["forecast_time"] = forecast_time
        result.append(row)
    return result


def transform_daily(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Pivot `daily` columnar data into a list of row-dicts."""
    daily = raw.get("daily", {})
    pivoted = _pivot_series(daily, DAILY_FIELD_MAP, "time", _parse_date)

    # sunrise/sunset need special handling: ISO datetime -> TIME
    sunrises = daily.get("sunrise") or []
    sunsets = daily.get("sunset") or []

    result = []
    for i, (forecast_date, row) in enumerate(pivoted):
        row["forecast_date"] = forecast_date
        row["sunrise"] = _parse_time_from_iso(sunrises[i]) if i < len(sunrises) else None
        row["sunset"]  = _parse_time_from_iso(sunsets[i])  if i < len(sunsets) else None
        result.append(row)
    return result


def transform_all(raw: dict[str, Any]) -> dict[str, Any]:
    """Transform a single city's raw API response into 4 DB-ready buckets."""
    return {
        "location": transform_location(raw),
        "current":  transform_current(raw),
        "hourly":   transform_hourly(raw),
        "daily":    transform_daily(raw),
    }


# -------------------------------------------------------------------
# Standalone test: python -m etl.transform
# -------------------------------------------------------------------

if __name__ == "__main__":
    from etl.extract import fetch_weather
    from etl.cities import CITIES

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Just test one city - Warsaw
    raw = fetch_weather(CITIES[0])
    transformed = transform_all(raw)

    print("\n=== LOCATION ===")
    print(transformed["location"])

    print("\n=== CURRENT ===")
    for k, v in transformed["current"].items():
        print(f"  {k:25s} = {v}")

    print(f"\n=== HOURLY (first 3 of {len(transformed['hourly'])} rows) ===")
    for row in transformed["hourly"][:3]:
        print(f"  {row['forecast_time']} | temp={row['temperature_c']}°C | "
              f"precip={row['precipitation_mm']}mm | wind={row['wind_speed_kmh']}km/h")

    print(f"\n=== DAILY ({len(transformed['daily'])} rows) ===")
    for row in transformed["daily"]:
        print(f"  {row['forecast_date']} | "
              f"min={row['temperature_min_c']}°C max={row['temperature_max_c']}°C | "
              f"sunrise={row['sunrise']} sunset={row['sunset']}")
