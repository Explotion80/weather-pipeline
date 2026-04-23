-- =============================================
-- Weather Pipeline - Database Schema
-- =============================================

-- Locations table: cities/points we track
CREATE TABLE IF NOT EXISTS locations (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    latitude        DECIMAL(8, 5) NOT NULL,
    longitude       DECIMAL(8, 5) NOT NULL,
    timezone        VARCHAR(50),
    elevation       DECIMAL(7, 2),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (latitude, longitude)
);

-- Current weather snapshots
CREATE TABLE IF NOT EXISTS current_weather (
    id                  SERIAL PRIMARY KEY,
    location_id         INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    recorded_at         TIMESTAMP NOT NULL,
    temperature_c       DECIMAL(5, 2),
    apparent_temp_c     DECIMAL(5, 2),
    humidity_pct        DECIMAL(5, 2),
    precipitation_mm    DECIMAL(7, 2),
    rain_mm             DECIMAL(7, 2),
    snowfall_cm         DECIMAL(7, 2),
    weather_code        INTEGER,
    cloud_cover_pct     DECIMAL(5, 2),
    pressure_hpa        DECIMAL(7, 2),
    wind_speed_kmh      DECIMAL(6, 2),
    wind_direction_deg  DECIMAL(5, 2),
    wind_gusts_kmh      DECIMAL(6, 2),
    is_day              BOOLEAN,
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_current_weather_location ON current_weather(location_id);
CREATE INDEX idx_current_weather_recorded ON current_weather(recorded_at);

-- Hourly forecast
CREATE TABLE IF NOT EXISTS hourly_forecast (
    id                          SERIAL PRIMARY KEY,
    location_id                 INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    forecast_time               TIMESTAMP NOT NULL,
    temperature_c               DECIMAL(5, 2),
    apparent_temp_c             DECIMAL(5, 2),
    humidity_pct                DECIMAL(5, 2),
    dew_point_c                 DECIMAL(5, 2),
    precipitation_mm            DECIMAL(7, 2),
    precipitation_probability   DECIMAL(5, 2),
    rain_mm                     DECIMAL(7, 2),
    snowfall_cm                 DECIMAL(7, 2),
    weather_code                INTEGER,
    cloud_cover_pct             DECIMAL(5, 2),
    visibility_m                DECIMAL(10, 2),
    wind_speed_kmh              DECIMAL(6, 2),
    wind_direction_deg          DECIMAL(5, 2),
    wind_gusts_kmh              DECIMAL(6, 2),
    fetched_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (location_id, forecast_time, fetched_at)
);

CREATE INDEX idx_hourly_location ON hourly_forecast(location_id);
CREATE INDEX idx_hourly_time ON hourly_forecast(forecast_time);

-- Daily forecast
CREATE TABLE IF NOT EXISTS daily_forecast (
    id                          SERIAL PRIMARY KEY,
    location_id                 INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    forecast_date               DATE NOT NULL,
    temperature_max_c           DECIMAL(5, 2),
    temperature_min_c           DECIMAL(5, 2),
    temperature_mean_c          DECIMAL(5, 2),
    precipitation_sum_mm        DECIMAL(7, 2),
    precipitation_probability   DECIMAL(5, 2),
    precipitation_hours         DECIMAL(5, 2),
    rain_sum_mm                 DECIMAL(7, 2),
    snowfall_sum_cm             DECIMAL(7, 2),
    weather_code                INTEGER,
    sunrise                     TIME,
    sunset                      TIME,
    sunshine_duration_s         DECIMAL(10, 2),
    wind_speed_max_kmh          DECIMAL(6, 2),
    wind_gusts_max_kmh          DECIMAL(6, 2),
    wind_direction_dominant_deg DECIMAL(5, 2),
    uv_index_max                DECIMAL(4, 2),
    fetched_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (location_id, forecast_date, fetched_at)
);

CREATE INDEX idx_daily_location ON daily_forecast(location_id);
CREATE INDEX idx_daily_date ON daily_forecast(forecast_date);
