package com.cinema.aggregation.domain;

import com.cinema.aggregation.clickhouse.ClickHouseQueryService;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.logging.AggregationLogger;
import com.cinema.aggregation.postgres.MetricsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Оркестратор вычисления бизнес-метрик.
 *
 * <p>Координирует:
 * <ol>
 *   <li>Чтение raw-событий из ClickHouse и вычисление агрегатов</li>
 *   <li>Идемпотентную запись результатов в PostgreSQL</li>
 * </ol>
 *
 * <p>Вычисляет метрики: DAU, avg_watch_seconds, conversion_rate, top_movie, retention D1/D7.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AggregationService {

    private final ClickHouseQueryService clickHouseQueryService;
    private final MetricsRepository metricsRepository;

    /**
     * Запускает полный цикл агрегации за указанную дату.
     *
     * <p>Использует один correlationId для всего цикла — все логи можно найти по нему.
     *
     * @param targetDate дата, за которую вычисляются агрегаты
     * @return Result с количеством записей или первой встреченной ошибкой
     */
    public Result<Integer, AppError> aggregate(LocalDate targetDate) {
        String correlationId = UUID.randomUUID().toString();
        long startMs = System.currentTimeMillis();

        AggregationLogger.logCycleStarted(correlationId, targetDate);
        metricsRepository.recordRunStarted(correlationId, targetDate);

        List<DailyMetric> allMetrics = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        // 1. DAU
        Result<DailyMetric, AppError> dauResult = clickHouseQueryService.computeDau(targetDate, correlationId);
        if (dauResult.isSuccess()) {
            allMetrics.add(dauResult.getValue());
        } else {
            errors.add("dau: " + dauResult.getError().message());
        }

        // 2. Среднее время просмотра
        Result<DailyMetric, AppError> watchResult = clickHouseQueryService.computeAvgWatchTime(targetDate, correlationId);
        if (watchResult.isSuccess()) {
            allMetrics.add(watchResult.getValue());
        } else {
            errors.add("avg_watch: " + watchResult.getError().message());
        }

        // 3. Конверсия
        Result<DailyMetric, AppError> convResult = clickHouseQueryService.computeConversionRate(targetDate, correlationId);
        if (convResult.isSuccess()) {
            allMetrics.add(convResult.getValue());
        } else {
            errors.add("conversion: " + convResult.getError().message());
        }

        // 4. Топ фильмов
        Result<List<DailyMetric>, AppError> topResult = clickHouseQueryService.computeTopMovies(targetDate, correlationId);
        if (topResult.isSuccess()) {
            allMetrics.addAll(topResult.getValue());
        } else {
            errors.add("top_movies: " + topResult.getError().message());
        }

        // 5. Retention D1/D7
        Result<List<DailyMetric>, AppError> retentionResult = clickHouseQueryService.computeRetention(targetDate, correlationId);
        if (retentionResult.isSuccess()) {
            allMetrics.addAll(retentionResult.getValue());
        } else {
            errors.add("retention: " + retentionResult.getError().message());
        }

        // Если все метрики провалились — возвращаем ошибку
        if (allMetrics.isEmpty()) {
            String combinedError = String.join("; ", errors);
            long durationMs = System.currentTimeMillis() - startMs;
            AggregationLogger.logCycleFailed(correlationId, targetDate, "ALL_METRICS_FAILED", combinedError);
            metricsRepository.recordRunFailed(correlationId, combinedError, durationMs);
            return Result.failure(AppError.of(com.cinema.aggregation.error.ErrorCode.AGGREGATION_FAILED, combinedError));
        }

        // Записываем в PostgreSQL
        Result<Integer, AppError> saveResult = metricsRepository.saveMetrics(allMetrics, correlationId);
        long durationMs = System.currentTimeMillis() - startMs;

        if (saveResult.isFailure()) {
            AggregationLogger.logCycleFailed(correlationId, targetDate,
                    saveResult.getError().code().getCode(), saveResult.getError().message());
            metricsRepository.recordRunFailed(correlationId, saveResult.getError().message(), durationMs);
            return saveResult;
        }

        int written = saveResult.getValue();
        if (!errors.isEmpty()) {
            log.warn("[AGGREGATION_PARTIAL] correlationId={} date={} skipped_metrics={}",
                    correlationId, targetDate, errors);
        }

        AggregationLogger.logCycleCompleted(correlationId, targetDate, written, durationMs);
        metricsRepository.recordRunCompleted(correlationId, written, durationMs);
        return Result.success(written);
    }
}
