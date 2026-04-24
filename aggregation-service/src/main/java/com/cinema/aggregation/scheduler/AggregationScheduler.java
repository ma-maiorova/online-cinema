package com.cinema.aggregation.scheduler;

import com.cinema.aggregation.domain.AggregationService;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.Result;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Планировщик цикличного запуска агрегации.
 *
 * <p>Интервал настраивается через {@code AGGREGATION_SCHEDULE_CRON} (env).
 * По умолчанию: каждый час (0 0 * * * *).
 *
 * <p>Агрегирует за предыдущий день (вчера), так как данные за текущий день
 * ещё могут поступать.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationScheduler {

    private final AggregationService aggregationService;

    @Value("${cinema.aggregation.lookback-days:1}")
    private int lookbackDays;

    /**
     * Запускается по cron-расписанию (настраивается через AGGREGATION_SCHEDULE_CRON).
     */
    @Scheduled(cron = "${cinema.aggregation.schedule-cron:0 0 * * * *}")
    public void runScheduled() {
        LocalDate targetDate = LocalDate.now().minusDays(lookbackDays);
        log.info("[SCHEDULER_TRIGGERED] target_date={}", targetDate);

        Result<Integer, AppError> result = aggregationService.aggregate(targetDate);

        if (result.isFailure()) {
            log.error("[SCHEDULER_FAILED] target_date={} error_code={} error={}",
                    targetDate, result.getError().code(), result.getError().message());
        } else {
            log.info("[SCHEDULER_SUCCESS] target_date={} records_written={}", targetDate, result.getValue());
        }
    }
}
