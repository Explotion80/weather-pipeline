"""
Extract module - fetches weather data from Open-Meteo API.

Open-Meteo is a free weather API that requires no API key.
Docs: https://open-meteo.com/en/docs
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from etl.cities import CITIES

logger = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 30  # seconds - raised from 10, API can be slow sometimes
MAX_RETRIES = 2       # total attempts per city on transient errors
RETRY_DELAY = 2       # seconds between retries

# Fields we want from each endpoint - mapped to our DB columns
CURRENT_FIELDS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
]

HOURLY_FIELDS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "precipitation_probability",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "precipitation_probability_max",
    "precipitation_hours",
    "rain_sum",
    "snowfall_sum",
    "weather_code",
    "sunrise",
    "sunset",
    "sunshine_duration",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "uv_index_max",
]


def fetch_weather(city: dict[str, Any]) -> dict[str, Any]:
    """
    Fetch current weather, hourly and daily forecast for a single city.

    Args:
        city: dict with keys 'name', 'latitude', 'longitude'.

    Returns:
        Raw JSON response as dict, with added 'city_name' key for convenience.

    Raises:
        requests.RequestException: on network or HTTP errors.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": ",".join(CURRENT_FIELDS),
        "hourly": ",".join(HOURLY_FIELDS),
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "auto",
    }

    logger.info("Fetching weather for %s", city["name"])

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()  # raises for HTTP 4xx/5xx
            data = response.json()
            data["city_name"] = city["name"]  # API doesn't know our name, attach for convenience
            return data
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                logger.warning(
                    "Attempt %d/%d failed for %s: %s - retrying in %ds",
                    attempt, MAX_RETRIES, city["name"], exc, RETRY_DELAY,
                )
                time.sleep(RETRY_DELAY)

    # All retries exhausted - re-raise the last transient error
    assert last_exc is not None
    raise last_exc


def fetch_all() -> list[dict[str, Any]]:
    """Fetch weather for all cities defined in CITIES. Skips cities on error."""
    results = []
    for city in CITIES:
        try:
            results.append(fetch_weather(city))
        except requests.RequestException as exc:
            logger.error("Failed to fetch %s: %s", city["name"], exc)
    return results


if __name__ == "__main__":
    # Standalone run: python -m etl.extract
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    for city_data in fetch_all():
        name = city_data["city_name"]
        current = city_data.get("current", {})
        temp = current.get("temperature_2m")
        wind = current.get("wind_speed_10m")
        print(f"{name:10s} | temp: {temp}°C | wind: {wind} km/h")
