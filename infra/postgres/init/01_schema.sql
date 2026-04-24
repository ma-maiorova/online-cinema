-- ─────────────────────────────────────────────────────────────────────────────
-- PostgreSQL: схема для хранения агрегированных метрик
-- Применяется автоматически при старте контейнера
-- ─────────────────────────────────────────────────────────────────────────────

-- Включаем расширение для UPSERT с ON CONFLICT DO UPDATE
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Главная таблица метрик: одна строка = одна метрика за один день
-- Идемпотентная запись через ON CONFLICT (metric_date, metric_name, dimension_key)
CREATE TABLE IF NOT EXISTS daily_metrics
(
    id             BIGSERIAL PRIMARY KEY,
    metric_date    DATE          NOT NULL,
    metric_name    VARCHAR(64)   NOT NULL,  -- dau, avg_watch_seconds, conversion_rate, retention_d1, retention_d7
    dimension_key  VARCHAR(256)  NOT NULL DEFAULT 'global',  -- для топ-фильмов = movie_id, иначе 'global'
    metric_value   DOUBLE PRECISION NOT NULL,
    extra_data     JSONB,                   -- доп. поля (cohort_size, rank, etc.)
    computed_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT uq_metric UNIQUE (metric_date, metric_name, dimension_key)
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics (metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_name ON daily_metrics (metric_name);

-- История запусков агрегации (для мониторинга)
CREATE TABLE IF NOT EXISTS aggregation_runs
(
    id              BIGSERIAL PRIMARY KEY,
    correlation_id  UUID          NOT NULL,
    target_date     DATE          NOT NULL,
    started_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(16)   NOT NULL DEFAULT 'RUNNING', -- RUNNING | SUCCESS | FAILED
    records_written INT,
    error_message   TEXT,
    duration_ms     BIGINT
);

CREATE INDEX IF NOT EXISTS idx_runs_target_date ON aggregation_runs (target_date DESC);
CREATE INDEX IF NOT EXISTS idx_runs_correlation ON aggregation_runs (correlation_id);

-- История экспортов в S3
CREATE TABLE IF NOT EXISTS s3_exports
(
    id              BIGSERIAL PRIMARY KEY,
    correlation_id  UUID          NOT NULL,
    export_date     DATE          NOT NULL,
    s3_path         VARCHAR(512)  NOT NULL,
    exported_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    status          VARCHAR(16)   NOT NULL DEFAULT 'SUCCESS', -- SUCCESS | FAILED
    error_message   TEXT,
    CONSTRAINT uq_s3_export UNIQUE (export_date)
);
