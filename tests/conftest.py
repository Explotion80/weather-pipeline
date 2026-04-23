"""
Shared pytest fixtures for all tests.

Fixtures defined here are auto-discovered - no imports needed in test files.

Overview:
  - sample_api_response / sample_transformed
      Test data from a real Open-Meteo response.

  - postgres_container / db_connection_params / db_schema / db_conn
      Ephemeral PostgreSQL via testcontainers. Session-scoped container +
      per-test TRUNCATE for isolation without restart cost.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from etl.transform import transform_all

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
FIXTURES_DIR = Path(__file__).parent / "fixtures"
INIT_SQL_PATH = PROJECT_ROOT / "sql" / "init.sql"

# All 4 tables - order matters for TRUNCATE with CASCADE
ALL_TABLES = ["current_weather", "hourly_forecast", "daily_forecast", "locations"]


# ===================================================================
# Sample data fixtures (used by unit tests)
# ===================================================================

@pytest.fixture
def sample_api_response() -> dict:
    """
    Real Open-Meteo API response for Warsaw (captured once, kept as fixture).

    Use this for transform tests - gives realistic data shape and values.
    The caller gets a fresh copy each time (tests can mutate without side effects).
    """
    with open(FIXTURES_DIR / "sample_response.json", encoding="utf-8") as f:
        data = json.load(f)
    # Attach city_name like fetch_weather() does
    data["city_name"] = "Warsaw"
    return data


@pytest.fixture
def sample_transformed(sample_api_response) -> dict:
    """
    Transformed data ready for load_city() - shortcut for integration tests.
    Shape: {"location": {...}, "current": {...}, "hourly": [...], "daily": [...]}
    """
    return transform_all(sample_api_response)


# ===================================================================
# Testcontainers - ephemeral PostgreSQL for integration tests
# ===================================================================

@pytest.fixture(scope="session")
def postgres_container():
    """
    Start PostgreSQL 16 in Docker ONCE for the whole test session.

    Startup takes ~5s - doing this per-test would explode wall-clock time.
    The container is torn down automatically after the session ends.
    """
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def db_connection_params(postgres_container) -> dict:
    """Extract connection params from the running container."""
    return {
        "host": postgres_container.get_container_host_ip(),
        "port": int(postgres_container.get_exposed_port(5432)),
        "user": postgres_container.username,
        "password": postgres_container.password,
        "dbname": postgres_container.dbname,
    }


@pytest.fixture(scope="session")
def db_schema(db_connection_params):
    """
    Apply sql/init.sql to create all 4 tables.

    Session-scoped because DDL doesn't change between tests - we just
    clean the data with TRUNCATE in the per-test fixture below.
    """
    sql = INIT_SQL_PATH.read_text(encoding="utf-8")
    with psycopg.connect(**db_connection_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        # `with conn:` in psycopg3 commits on success and closes
    return True  # marker value - tests only care that this fixture ran


@pytest.fixture
def db_conn(db_connection_params, db_schema):
    """
    Per-test connection with clean tables.

    TRUNCATE RESTART IDENTITY CASCADE:
      - removes all rows
      - resets SERIAL/IDENTITY sequences (so location.id starts at 1 again)
      - cascades to child tables (current_weather etc. via FK)

    CASCADE on locations alone would be enough, but listing all tables
    is clearer about intent.
    """
    conn = psycopg.connect(**db_connection_params)
    try:
        with conn.cursor() as cur:
            tables_sql = ", ".join(ALL_TABLES)
            cur.execute(f"TRUNCATE {tables_sql} RESTART IDENTITY CASCADE")
        conn.commit()
        yield conn
    finally:
        conn.close()
