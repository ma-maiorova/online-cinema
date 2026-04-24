-- ─────────────────────────────────────────────────────────────────────────────
-- ClickHouse: инициализация схемы для Online Cinema Analytics
-- Применяется автоматически при старте контейнера
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Таблица с Kafka Engine — читает raw-события из топика movie-events
--    Данные НЕ хранятся здесь постоянно, только буферизуются
CREATE TABLE IF NOT EXISTS cinema.movie_events_kafka
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       String,   -- VIEW_STARTED | VIEW_FINISHED | VIEW_PAUSED | VIEW_RESUMED | LIKED | SEARCHED
    event_timestamp  DateTime,
    device_type      String,   -- MOBILE | DESKTOP | TV | TABLET
    session_id       String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9094',
    kafka_topic_list  = 'movie-events',
    kafka_group_name  = 'clickhouse-consumer',
    kafka_format      = 'Avro',
    kafka_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536;

-- 2. Основная таблица MergeTree — постоянное хранилище raw-событий
--    Партиционирование по месяцу для эффективного DELETE старых данных
--    ORDER BY (event_date, user_id) — оптимально для retention-запросов
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         String,
    user_id          String,
    movie_id         String,
    event_type       LowCardinality(String),
    event_timestamp  DateTime,
    event_date       Date DEFAULT toDate(event_timestamp),
    device_type      LowCardinality(String),
    session_id       String,
    progress_seconds Int32,
    ingested_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_timestamp)
TTL event_date + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- 3. Materialized View — автоматически перекладывает данные из Kafka в MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    event_timestamp,
    toDate(event_timestamp) AS event_date,
    device_type,
    session_id,
    progress_seconds,
    now()                   AS ingested_at
FROM cinema.movie_events_kafka;

-- ─── Агрегированные таблицы (блок 2) ────────────────────────────────────────

-- 4. DAU (Daily Active Users)
CREATE TABLE IF NOT EXISTS cinema.agg_dau
(
    event_date   Date,
    dau          UInt64,
    computed_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- 5. Среднее время просмотра (VIEW_FINISHED)
CREATE TABLE IF NOT EXISTS cinema.agg_avg_watch_time
(
    event_date        Date,
    avg_watch_seconds Float64,
    total_views       UInt64,
    computed_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- 6. Топ фильмов по просмотрам
CREATE TABLE IF NOT EXISTS cinema.agg_top_movies
(
    event_date   Date,
    movie_id     String,
    view_count   UInt64,
    rank         UInt32,
    computed_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, rank);

-- 7. Конверсия просмотра (VIEW_FINISHED / VIEW_STARTED)
CREATE TABLE IF NOT EXISTS cinema.agg_conversion
(
    event_date        Date,
    started_count     UInt64,
    finished_count    UInt64,
    conversion_rate   Float64,
    computed_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY event_date;

-- 8. Retention D1 / D7 по когортам
CREATE TABLE IF NOT EXISTS cinema.agg_retention
(
    cohort_date      Date,
    day_number       UInt8,   -- 0, 1, 7
    cohort_size      UInt64,
    retained_count   UInt64,
    retention_rate   Float64,
    computed_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number);
