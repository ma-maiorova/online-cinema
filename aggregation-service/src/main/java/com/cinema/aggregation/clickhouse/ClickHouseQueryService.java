package com.cinema.aggregation.clickhouse;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.logging.AggregationLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Сервис запросов к ClickHouse для вычисления бизнес-метрик.
 *
 * <p>Все методы возвращают {@link Result} — не бросают исключений.
 * Используют агрегатные функции ClickHouse: uniq(), avg(), sum(), rank().
 *
 * <p>Метрики:
 * <ul>
 *   <li>DAU — uniq(user_id) за день</li>
 *   <li>Среднее время просмотра — avg(progress_seconds) для VIEW_FINISHED</li>
 *   <li>Топ фильмов — count(*) GROUP BY movie_id + rank()</li>
 *   <li>Конверсия — countIf(VIEW_FINISHED) / countIf(VIEW_STARTED)</li>
 *   <li>Retention D1/D7 — когортный анализ через subquery</li>
 * </ul>
 */
@Slf4j
@Service
public class ClickHouseQueryService {

    private static final int TOP_MOVIES_LIMIT = 10;

    private final DataSource clickHouseDataSource;

    public ClickHouseQueryService(@Qualifier("clickHouseDataSource") DataSource clickHouseDataSource) {
        this.clickHouseDataSource = clickHouseDataSource;
    }

    /**
     * DAU: количество уникальных пользователей за день.
     */
    public Result<DailyMetric, AppError> computeDau(LocalDate date, String correlationId) {
        String sql = """
                SELECT uniq(user_id) AS dau
                FROM cinema.movie_events
                WHERE event_date = ?
                """;

        return executeScalar(sql, date, correlationId, rs -> {
            long dau = rs.getLong("dau");
            AggregationLogger.logMetricComputed(correlationId, "dau", date, dau);
            return DailyMetric.global("dau", date, dau);
        });
    }

    /**
     * Среднее время просмотра в секундах для событий VIEW_FINISHED.
     */
    public Result<DailyMetric, AppError> computeAvgWatchTime(LocalDate date, String correlationId) {
        String sql = """
                SELECT avg(progress_seconds) AS avg_watch_seconds,
                       count() AS total_views
                FROM cinema.movie_events
                WHERE event_date = ?
                  AND event_type = 'VIEW_FINISHED'
                """;

        return executeScalar(sql, date, correlationId, rs -> {
            double avg = rs.getDouble("avg_watch_seconds");
            AggregationLogger.logMetricComputed(correlationId, "avg_watch_seconds", date, avg);
            return DailyMetric.global("avg_watch_seconds", date, avg);
        });
    }

    /**
     * Конверсия: доля VIEW_FINISHED от VIEW_STARTED за день.
     */
    public Result<DailyMetric, AppError> computeConversionRate(LocalDate date, String correlationId) {
        String sql = """
                SELECT
                    countIf(event_type = 'VIEW_STARTED')  AS started_count,
                    countIf(event_type = 'VIEW_FINISHED') AS finished_count,
                    if(countIf(event_type = 'VIEW_STARTED') > 0,
                       countIf(event_type = 'VIEW_FINISHED') / countIf(event_type = 'VIEW_STARTED'),
                       0) AS conversion_rate
                FROM cinema.movie_events
                WHERE event_date = ?
                """;

        return executeScalar(sql, date, correlationId, rs -> {
            double rate = rs.getDouble("conversion_rate");
            AggregationLogger.logMetricComputed(correlationId, "conversion_rate", date, rate);
            return DailyMetric.global("conversion_rate", date, rate);
        });
    }

    /**
     * Топ-10 фильмов по количеству просмотров за день.
     */
    public Result<List<DailyMetric>, AppError> computeTopMovies(LocalDate date, String correlationId) {
        String sql = """
                SELECT
                    movie_id,
                    count() AS view_count,
                    row_number() OVER (ORDER BY count() DESC) AS rank
                FROM cinema.movie_events
                WHERE event_date = ?
                  AND event_type IN ('VIEW_STARTED', 'VIEW_FINISHED')
                GROUP BY movie_id
                ORDER BY view_count DESC
                LIMIT ?
                """;

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, date.toString());
            stmt.setInt(2, TOP_MOVIES_LIMIT);

            List<DailyMetric> metrics = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String movieId = rs.getString("movie_id");
                    long viewCount = rs.getLong("view_count");
                    int rank = rs.getInt("rank");

                    metrics.add(DailyMetric.withDimension(
                            "top_movie", date, movieId, viewCount,
                            Map.of("rank", rank, "view_count", viewCount)
                    ));
                }
            }

            log.info("[CLICKHOUSE_TOP_MOVIES] date={} count={}", date, metrics.size());
            return Result.success(metrics);

        } catch (SQLException e) {
            log.error("[CLICKHOUSE_ERROR] metric=top_movies date={} error={}", date, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, e));
        }
    }

    /**
     * Retention D1 и D7: доля пользователей, вернувшихся через 1 и 7 дней.
     *
     * <p>Алгоритм:
     * <ol>
     *   <li>Находим дату первого события для каждого пользователя (cohort_date)</li>
     *   <li>Считаем пользователей cohort_date = date (cohort_size)</li>
     *   <li>Считаем из них тех, кто активен в date+1 (retention D1)</li>
     *   <li>Считаем из них тех, кто активен в date+7 (retention D7)</li>
     * </ol>
     */
    public Result<List<DailyMetric>, AppError> computeRetention(LocalDate date, String correlationId) {
        String sql = """
                WITH cohort AS (
                    SELECT user_id,
                           min(event_date) AS cohort_date
                    FROM cinema.movie_events
                    GROUP BY user_id
                    HAVING cohort_date = ?
                ),
                cohort_size AS (
                    SELECT count() AS size FROM cohort
                ),
                d1_retained AS (
                    SELECT count(DISTINCT e.user_id) AS count
                    FROM cinema.movie_events e
                    INNER JOIN cohort c ON e.user_id = c.user_id
                    WHERE e.event_date = toDate(?) + INTERVAL 1 DAY
                ),
                d7_retained AS (
                    SELECT count(DISTINCT e.user_id) AS count
                    FROM cinema.movie_events e
                    INNER JOIN cohort c ON e.user_id = c.user_id
                    WHERE e.event_date = toDate(?) + INTERVAL 7 DAY
                )
                SELECT
                    (SELECT size  FROM cohort_size)   AS cohort_size,
                    (SELECT count FROM d1_retained)   AS d1_count,
                    (SELECT count FROM d7_retained)   AS d7_count
                """;

        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, date.toString());
            stmt.setString(2, date.toString());
            stmt.setString(3, date.toString());

            List<DailyMetric> metrics = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Result.success(metrics);
                }

                long cohortSize = rs.getLong("cohort_size");
                long d1Count = rs.getLong("d1_count");
                long d7Count = rs.getLong("d7_count");

                double d1Rate = cohortSize > 0 ? (double) d1Count / cohortSize : 0.0;
                double d7Rate = cohortSize > 0 ? (double) d7Count / cohortSize : 0.0;

                metrics.add(DailyMetric.withDimension("retention_rate", date, "d1", d1Rate,
                        Map.of("cohort_size", cohortSize, "retained_count", d1Count, "day_number", 1)));
                metrics.add(DailyMetric.withDimension("retention_rate", date, "d7", d7Rate,
                        Map.of("cohort_size", cohortSize, "retained_count", d7Count, "day_number", 7)));

                AggregationLogger.logMetricComputed(correlationId, "retention_d1", date, d1Rate);
                AggregationLogger.logMetricComputed(correlationId, "retention_d7", date, d7Rate);
            }

            return Result.success(metrics);

        } catch (SQLException e) {
            log.error("[CLICKHOUSE_ERROR] metric=retention date={} error={}", date, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, e));
        }
    }

    // ─── Вспомогательный метод для скалярных запросов ────────────────────────

    @FunctionalInterface
    private interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    private <T> Result<T, AppError> executeScalar(
            String sql, LocalDate date, String correlationId, RowMapper<DailyMetric> mapper
    ) {
        try (Connection conn = clickHouseDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, date.toString());

            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return Result.failure(AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED,
                            "Пустой результат для date=" + date));
                }
                @SuppressWarnings("unchecked")
                T result = (T) mapper.map(rs);
                return Result.success(result);
            }
        } catch (SQLException e) {
            log.error("[CLICKHOUSE_ERROR] sql={} date={} error={}", sql.lines().findFirst().orElse("?"), date, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, e));
        }
    }
}
