package com.cinema.aggregation.postgres;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.logging.AggregationLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Репозиторий для идемпотентной записи метрик в PostgreSQL.
 *
 * <p>Использует {@code ON CONFLICT (metric_date, metric_name, dimension_key) DO UPDATE}
 * — повторный пересчёт за ту же дату обновляет значения, не создаёт дубликаты.
 *
 * <p>Все операции возвращают {@link Result} — не бросают исключений (согласно rules.md).
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class MetricsRepository {

    private static final String UPSERT_METRIC = """
            INSERT INTO daily_metrics (metric_date, metric_name, dimension_key, metric_value, extra_data, computed_at)
            VALUES (?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (metric_date, metric_name, dimension_key)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                extra_data   = EXCLUDED.extra_data,
                computed_at  = EXCLUDED.computed_at
            """;

    private static final String INSERT_RUN = """
            INSERT INTO aggregation_runs (correlation_id, target_date, started_at, status)
            VALUES (?::uuid, ?, now(), 'RUNNING')
            RETURNING id
            """;

    private static final String UPDATE_RUN = """
            UPDATE aggregation_runs
            SET finished_at     = now(),
                status          = ?,
                records_written = ?,
                error_message   = ?,
                duration_ms     = ?
            WHERE correlation_id = ?::uuid
            """;

    private static final String UPSERT_S3_EXPORT = """
            INSERT INTO s3_exports (correlation_id, export_date, s3_path, exported_at, status)
            VALUES (?::uuid, ?, ?, now(), 'SUCCESS')
            ON CONFLICT (export_date)
            DO UPDATE SET
                correlation_id = EXCLUDED.correlation_id,
                s3_path        = EXCLUDED.s3_path,
                exported_at    = EXCLUDED.exported_at,
                status         = EXCLUDED.status,
                error_message  = NULL
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Идемпотентно сохраняет список метрик в PostgreSQL.
     *
     * @param metrics список метрик
     * @param correlationId ID корреляции для логирования
     * @return Result с количеством записанных строк или ошибкой
     */
    @Transactional
    public Result<Integer, AppError> saveMetrics(List<DailyMetric> metrics, String correlationId) {
        if (metrics.isEmpty()) {
            return Result.success(0);
        }

        try {
            int written = 0;
            for (DailyMetric metric : metrics) {
                String extraJson = serializeExtra(metric);
                jdbcTemplate.update(UPSERT_METRIC,
                        metric.metricDate(),
                        metric.metricName(),
                        metric.dimensionKey(),
                        metric.metricValue(),
                        extraJson,
                        metric.computedAt()
                );
                written++;
            }

            LocalDate date = metrics.get(0).metricDate();
            AggregationLogger.logPostgresWriteSuccess(correlationId, date, written);
            return Result.success(written);

        } catch (Exception e) {
            LocalDate date = metrics.isEmpty() ? LocalDate.now() : metrics.get(0).metricDate();
            AggregationLogger.logPostgresWriteFailure(correlationId, date, e.getMessage());
            log.error("[POSTGRES_ERROR] correlationId={} error={}", correlationId, e.getMessage(), e);
            return Result.failure(AppError.of(ErrorCode.POSTGRES_WRITE_FAILED, e));
        }
    }

    /**
     * Записывает начало запуска агрегации.
     */
    public Result<Void, AppError> recordRunStarted(String correlationId, LocalDate targetDate) {
        try {
            jdbcTemplate.update(INSERT_RUN, correlationId, targetDate);
            return Result.success(null);
        } catch (Exception e) {
            log.warn("[POSTGRES_RUN_LOG_WARN] Не удалось записать начало запуска: {}", e.getMessage());
            // Не возвращаем ошибку — лог запуска не критичен
            return Result.success(null);
        }
    }

    /**
     * Обновляет статус запуска агрегации по завершении.
     */
    public void recordRunCompleted(String correlationId, int recordsWritten, long durationMs) {
        try {
            jdbcTemplate.update(UPDATE_RUN, "SUCCESS", recordsWritten, null, durationMs, correlationId);
        } catch (Exception e) {
            log.warn("[POSTGRES_RUN_LOG_WARN] Не удалось обновить статус запуска: {}", e.getMessage());
        }
    }

    /**
     * Обновляет статус запуска агрегации при ошибке.
     */
    public void recordRunFailed(String correlationId, String errorMessage, long durationMs) {
        try {
            jdbcTemplate.update(UPDATE_RUN, "FAILED", 0, errorMessage, durationMs, correlationId);
        } catch (Exception e) {
            log.warn("[POSTGRES_RUN_LOG_WARN] Не удалось записать ошибку запуска: {}", e.getMessage());
        }
    }

    /**
     * Идемпотентно записывает информацию об экспорте в S3.
     * Повторный экспорт за ту же дату обновляет запись.
     */
    public Result<Void, AppError> recordS3Export(LocalDate exportDate, String s3Path) {
        String correlationId = UUID.randomUUID().toString();
        try {
            jdbcTemplate.update(UPSERT_S3_EXPORT, correlationId, exportDate, s3Path);
            return Result.success(null);
        } catch (Exception e) {
            log.error("[POSTGRES_S3_LOG_ERROR] date={} error={}", exportDate, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.POSTGRES_WRITE_FAILED, e));
        }
    }

    /**
     * Читает метрики из PostgreSQL за конкретную дату (для экспорта в S3).
     */
    public Result<List<DailyMetric>, AppError> findMetricsByDate(LocalDate date) {
        String sql = """
                SELECT metric_date, metric_name, dimension_key, metric_value, extra_data, computed_at
                FROM daily_metrics
                WHERE metric_date = ?
                ORDER BY metric_name, dimension_key
                """;
        try {
            List<DailyMetric> metrics = jdbcTemplate.query(sql,
                    (rs, rowNum) -> new DailyMetric(
                            rs.getDate("metric_date").toLocalDate(),
                            rs.getString("metric_name"),
                            rs.getString("dimension_key"),
                            rs.getDouble("metric_value"),
                            null, // extra_data не нужен для экспорта
                            rs.getObject("computed_at", java.time.OffsetDateTime.class)
                    ),
                    date
            );
            return Result.success(metrics);
        } catch (Exception e) {
            log.error("[POSTGRES_READ_ERROR] date={} error={}", date, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.POSTGRES_CONNECTION_FAILED, e));
        }
    }

    private String serializeExtra(DailyMetric metric) {
        if (metric.extraData() == null || metric.extraData().isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(metric.extraData());
        } catch (JsonProcessingException e) {
            log.warn("[SERIALIZE_WARN] Не удалось сериализовать extra_data: {}", e.getMessage());
            return null;
        }
    }
}
