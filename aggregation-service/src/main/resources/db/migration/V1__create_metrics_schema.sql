-- ─────────────────────────────────────────────────────────────────────────────
-- V1: Начальная схема PostgreSQL для хранения агрегатов
-- Применяется Flyway автоматически при старте Aggregation Service
-- ─────────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Основная таблица метрик
-- Идемпотентная запись через ON CONFLICT (metric_date, metric_name, dimension_key)
CREATE TABLE IF NOT EXISTS daily_metrics
(
    id             BIGSERIAL        PRIMARY KEY,
    metric_date    DATE             NOT NULL,
    metric_name    VARCHAR(64)      NOT NULL,
    -- Для топ-фильмов: movie_id. Для retention: 'd1' / 'd7'. Иначе: 'global'
    dimension_key  VARCHAR(256)     NOT NULL DEFAULT 'global',
    metric_value   DOUBLE PRECISION NOT NULL,
    -- Дополнительные поля (cohort_size, rank и т.д.)
    extra_data     JSONB,
    computed_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT uq_metric UNIQUE (metric_date, metric_name, dimension_key)
);

CREATE INDEX idx_daily_metrics_date ON daily_metrics (metric_date DESC);
CREATE INDEX idx_daily_metrics_name ON daily_metrics (metric_name);
CREATE INDEX idx_daily_metrics_date_name ON daily_metrics (metric_date, metric_name);

-- История запусков агрегации
CREATE TABLE IF NOT EXISTS aggregation_runs
(
    id              BIGSERIAL    PRIMARY KEY,
    correlation_id  UUID         NOT NULL,
    target_date     DATE         NOT NULL,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(16)  NOT NULL DEFAULT 'RUNNING',
    records_written INT,
    error_message   TEXT,
    duration_ms     BIGINT
);

CREATE INDEX idx_runs_target_date    ON aggregation_runs (target_date DESC);
CREATE INDEX idx_runs_correlation_id ON aggregation_runs (correlation_id);
CREATE INDEX idx_runs_status         ON aggregation_runs (status);

-- История экспортов в S3
CREATE TABLE IF NOT EXISTS s3_exports
(
    id              BIGSERIAL    PRIMARY KEY,
    correlation_id  UUID         NOT NULL,
    export_date     DATE         NOT NULL,
    s3_path         VARCHAR(512) NOT NULL,
    exported_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    status          VARCHAR(16)  NOT NULL DEFAULT 'SUCCESS',
    error_message   TEXT,
    CONSTRAINT uq_s3_export UNIQUE (export_date)
);

CREATE INDEX idx_s3_exports_date ON s3_exports (export_date DESC);
