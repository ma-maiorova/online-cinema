package com.cinema.aggregation.domain;

import jakarta.annotation.Nullable;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Агрегированная метрика за конкретный день.
 *
 * <p>Хранится в PostgreSQL таблице {@code daily_metrics}.
 * Идемпотентная запись — ON CONFLICT (metric_date, metric_name, dimension_key) DO UPDATE.
 *
 * @param metricDate   дата, за которую вычислена метрика
 * @param metricName   название метрики (dau, avg_watch_seconds, conversion_rate, retention_d1, retention_d7, top_movie)
 * @param dimensionKey для top_movie = movie_id; для retention = d1/d7; иначе = "global"
 * @param metricValue  числовое значение метрики
 * @param extraData    дополнительные поля (rank, cohort_size и т.д.)
 * @param computedAt   время вычисления
 */
public record DailyMetric(
        LocalDate metricDate,
        String metricName,
        String dimensionKey,
        double metricValue,
        @Nullable Map<String, Object> extraData,
        OffsetDateTime computedAt
) {
    /**
     * Создаёт метрику с dimension_key = "global".
     */
    public static DailyMetric global(String metricName, LocalDate date, double value) {
        return new DailyMetric(date, metricName, "global", value, null, OffsetDateTime.now());
    }

    /**
     * Создаёт метрику с произвольным dimension_key.
     */
    public static DailyMetric withDimension(String metricName, LocalDate date,
                                             String dimensionKey, double value,
                                             @Nullable Map<String, Object> extraData) {
        return new DailyMetric(date, metricName, dimensionKey, value, extraData, OffsetDateTime.now());
    }
}
