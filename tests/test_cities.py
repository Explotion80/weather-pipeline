"""
Unit tests for etl/cities.py

cities.py is just a static list, but these tests enforce a 'data contract':
no missing keys, no duplicates, valid coordinate ranges, correct types.
Cheap insurance against typos when adding new cities.
"""
from __future__ import annotations

from etl.cities import CITIES

# Bounding box for Poland (with a small margin). Adjust if you add cities
# outside Poland - that would be a conscious decision, not a typo.
POLAND_LAT_MIN, POLAND_LAT_MAX = 48.0, 55.0
POLAND_LON_MIN, POLAND_LON_MAX = 14.0, 25.0


class TestCitiesListShape:
    def test_is_non_empty_list(self):
        assert isinstance(CITIES, list)
        assert len(CITIES) > 0

    def test_every_entry_is_a_dict(self):
        for city in CITIES:
            assert isinstance(city, dict)

    def test_every_city_has_required_keys(self):
        required = {"name", "latitude", "longitude"}
        for city in CITIES:
            missing = required - city.keys()
            assert not missing, f"{city} missing keys: {missing}"


class TestCitiesDataQuality:
    def test_names_are_non_empty_strings(self):
        for city in CITIES:
            assert isinstance(city["name"], str)
            assert city["name"].strip(), f"empty name in {city}"

    def test_names_are_unique(self):
        # Duplicate names would cause UPSERT collisions downstream
        names = [c["name"] for c in CITIES]
        assert len(names) == len(set(names)), f"duplicate name in {names}"

    def test_coordinates_are_numeric(self):
        # Catches typos like {"latitude": "52.23"} (string instead of float)
        for city in CITIES:
            assert isinstance(city["latitude"], (int, float))
            assert isinstance(city["longitude"], (int, float))

    def test_coordinates_are_within_poland(self):
        # Sanity bounds - catches swapped lat/lon or decimal typos
        for city in CITIES:
            lat, lon = city["latitude"], city["longitude"]
            assert POLAND_LAT_MIN <= lat <= POLAND_LAT_MAX, (
                f"{city['name']}: latitude {lat} outside Poland"
            )
            assert POLAND_LON_MIN <= lon <= POLAND_LON_MAX, (
                f"{city['name']}: longitude {lon} outside Poland"
            )

    def test_coordinate_pairs_are_unique(self):
        # Two cities at the same (lat, lon) would collide on the UNIQUE constraint
        pairs = [(c["latitude"], c["longitude"]) for c in CITIES]
        assert len(pairs) == len(set(pairs)), "duplicate coordinates"
