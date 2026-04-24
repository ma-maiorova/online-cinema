package com.cinema.aggregation.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.LocalDate;

/**
 * Утилита для структурированного логирования событий агрегации с correlationId.
 *
 * <p>Все методы добавляют {@code correlationId} в MDC для сквозной трассировки.
 */
public class AggregationLogger {

    private static final Logger log = LoggerFactory.getLogger(AggregationLogger.class);
    private static final String MDC_KEY = CorrelationIdFilter.MDC_KEY;

    private AggregationLogger() {}

    /**
     * Логирует начало цикла агрегации.
     */
    public static void logCycleStarted(String correlationId, LocalDate targetDate) {
        withCorrelationId(correlationId, () ->
                log.info("[AGGREGATION_STARTED] target_date={} correlation_id={}", targetDate, correlationId)
        );
    }

    /**
     * Логирует успешное завершение цикла агрегации.
     *
     * @param recordsWritten количество записей, записанных в PostgreSQL
     * @param durationMs     время выполнения в миллисекундах
     */
    public static void logCycleCompleted(String correlationId, LocalDate targetDate,
                                         int recordsWritten, long durationMs) {
        withCorrelationId(correlationId, () ->
                log.info("[AGGREGATION_COMPLETED] target_date={} records_written={} duration_ms={}",
                        targetDate, recordsWritten, durationMs)
        );
    }

    /**
     * Логирует ошибку цикла агрегации.
     */
    public static void logCycleFailed(String correlationId, LocalDate targetDate,
                                      String errorCode, String errorMessage) {
        withCorrelationId(correlationId, () ->
                log.error("[AGGREGATION_FAILED] target_date={} error_code={} error={}",
                        targetDate, errorCode, errorMessage)
        );
    }

    /**
     * Логирует результат вычисления конкретной метрики.
     */
    public static void logMetricComputed(String correlationId, String metricName,
                                         LocalDate date, double value) {
        withCorrelationId(correlationId, () ->
                log.info("[METRIC_COMPUTED] metric={} date={} value={}", metricName, date, value)
        );
    }

    /**
     * Логирует успешную запись в PostgreSQL.
     */
    public static void logPostgresWriteSuccess(String correlationId, LocalDate date, int count) {
        withCorrelationId(correlationId, () ->
                log.info("[POSTGRES_WRITE_SUCCESS] date={} records={}", date, count)
        );
    }

    /**
     * Логирует ошибку записи в PostgreSQL.
     */
    public static void logPostgresWriteFailure(String correlationId, LocalDate date, String error) {
        withCorrelationId(correlationId, () ->
                log.error("[POSTGRES_WRITE_FAILURE] date={} error={}", date, error)
        );
    }

    /**
     * Логирует успешный экспорт в S3.
     */
    public static void logS3ExportSuccess(String correlationId, LocalDate date, String s3Path) {
        withCorrelationId(correlationId, () ->
                log.info("[S3_EXPORT_SUCCESS] date={} path={}", date, s3Path)
        );
    }

    /**
     * Логирует ошибку экспорта в S3.
     */
    public static void logS3ExportFailure(String correlationId, LocalDate date, String error) {
        withCorrelationId(correlationId, () ->
                log.error("[S3_EXPORT_FAILURE] date={} error={}", date, error)
        );
    }

    private static void withCorrelationId(String correlationId, Runnable action) {
        String existing = MDC.get(MDC_KEY);
        MDC.put(MDC_KEY, correlationId != null ? correlationId : "scheduler");
        try {
            action.run();
        } finally {
            if (existing != null) {
                MDC.put(MDC_KEY, existing);
            } else {
                MDC.remove(MDC_KEY);
            }
        }
    }
}
