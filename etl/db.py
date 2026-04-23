"""
Database connection helpers.

Reads Postgres credentials from .env (loaded once at import time)
and exposes get_connection() for the rest of the pipeline.

Uses psycopg (v3) - modern successor to psycopg2 with better Unicode support.
"""
from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)  # .env beats existing shell env vars


def get_connection() -> psycopg.Connection:
    """Return a new psycopg connection using .env credentials."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        application_name="weather_etl",
    )
