package com.cinema.aggregation.postgres;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for MetricsRepository using a real PostgreSQL container.
 * Flyway migrations are applied automatically via Spring Boot test slice.
 */
@JdbcTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(MetricsRepositoryTest.TestConfig.class)
@DisplayName("Metrics Repository Integration Tests")
class MetricsRepositoryTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("cinema_metrics_test")
            .withUsername("cinema")
            .withPassword("cinema_pass");

    @DynamicPropertySource
    static void configureDataSource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
    }

    @Configuration
    static class TestConfig {
        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        MetricsRepository metricsRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
            return new MetricsRepository(jdbcTemplate, objectMapper);
        }
    }

    @Autowired
    private MetricsRepository metricsRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final LocalDate TEST_DATE = LocalDate.of(2024, 1, 15);
    private static final String CORRELATION_ID = "test-correlation-" + System.nanoTime();

    // ─── saveMetrics ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("saveMetrics inserts metrics correctly and returns count")
    void shouldSaveMetricsAndReturnCount() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", TEST_DATE, 1000.0),
                DailyMetric.global("avg_watch_seconds", TEST_DATE, 1800.5),
                DailyMetric.global("conversion_rate", TEST_DATE, 0.65)
        );

        // When
        Result<Integer, AppError> result = metricsRepository.saveMetrics(metrics, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(3);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM daily_metrics WHERE metric_date = ?", Integer.class, TEST_DATE);
        assertThat(count).isEqualTo(3);
    }

    @Test
    @DisplayName("saveMetrics is idempotent: re-saving same key updates value")
    void shouldUpdateMetricOnConflict() {
        // Given - save initial value
        LocalDate date = LocalDate.of(2024, 2, 1);
        List<DailyMetric> first = List.of(DailyMetric.global("dau", date, 500.0));
        metricsRepository.saveMetrics(first, CORRELATION_ID);

        // When - re-save with updated value for same (date, name, dimension)
        List<DailyMetric> updated = List.of(DailyMetric.global("dau", date, 750.0));
        Result<Integer, AppError> result = metricsRepository.saveMetrics(updated, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();

        Double value = jdbcTemplate.queryForObject(
                "SELECT metric_value FROM daily_metrics WHERE metric_date = ? AND metric_name = 'dau' AND dimension_key = 'global'",
                Double.class, date);
        assertThat(value).isEqualTo(750.0);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM daily_metrics WHERE metric_date = ? AND metric_name = 'dau'",
                Integer.class, date);
        assertThat(count).isEqualTo(1); // no duplicate
    }

    @Test
    @DisplayName("saveMetrics with empty list returns success(0) without DB writes")
    void shouldReturnZeroForEmptyMetricsList() {
        // When
        Result<Integer, AppError> result = metricsRepository.saveMetrics(List.of(), CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(0);
    }

    @Test
    @DisplayName("saveMetrics stores dimension metrics with extraData correctly")
    void shouldSaveDimensionMetricsWithExtraData() {
        // Given
        LocalDate date = LocalDate.of(2024, 3, 1);
        List<DailyMetric> metrics = List.of(
                DailyMetric.withDimension("top_movie", date, "movie-abc", 200.0, Map.of("rank", 1)),
                DailyMetric.withDimension("retention_rate", date, "d1", 0.45, Map.of("cohort_size", 1000L))
        );

        // When
        Result<Integer, AppError> result = metricsRepository.saveMetrics(metrics, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(2);

        Integer topMovieCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM daily_metrics WHERE metric_date = ? AND metric_name = 'top_movie' AND dimension_key = 'movie-abc'",
                Integer.class, date);
        assertThat(topMovieCount).isEqualTo(1);
    }

    // ─── findMetricsByDate ────────────────────────────────────────────────────

    @Test
    @DisplayName("findMetricsByDate returns all metrics for a date ordered by name and dimension")
    void shouldFindMetricsByDate() {
        // Given
        LocalDate date = LocalDate.of(2024, 4, 1);
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", date, 1000.0),
                DailyMetric.global("avg_watch_seconds", date, 900.0),
                DailyMetric.withDimension("top_movie", date, "movie-1", 50.0, null)
        );
        metricsRepository.saveMetrics(metrics, CORRELATION_ID);

        // When
        Result<List<DailyMetric>, AppError> result = metricsRepository.findMetricsByDate(date);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).hasSize(3);

        List<String> names = result.getValue().stream().map(DailyMetric::metricName).toList();
        assertThat(names).containsExactlyInAnyOrder("dau", "avg_watch_seconds", "top_movie");
    }

    @Test
    @DisplayName("findMetricsByDate returns empty list for date with no data")
    void shouldReturnEmptyListForDateWithNoData() {
        // When
        Result<List<DailyMetric>, AppError> result =
                metricsRepository.findMetricsByDate(LocalDate.of(2099, 12, 31));

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEmpty();
    }

    // ─── aggregation_runs lifecycle ───────────────────────────────────────────

    @Test
    @DisplayName("recordRunStarted inserts a RUNNING record in aggregation_runs")
    void shouldRecordRunStarted() {
        // Given
        String corrId = "run-started-" + System.nanoTime();
        LocalDate date = LocalDate.of(2024, 5, 1);

        // When
        Result<Void, AppError> result = metricsRepository.recordRunStarted(corrId, date);

        // Then
        assertThat(result.isSuccess()).isTrue();
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM aggregation_runs WHERE target_date = ? AND status = 'RUNNING'",
                Integer.class, date);
        assertThat(count).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("recordRunCompleted updates status to SUCCESS")
    void shouldRecordRunCompleted() {
        // Given
        String corrId = "run-completed-" + System.nanoTime();
        LocalDate date = LocalDate.of(2024, 6, 1);
        metricsRepository.recordRunStarted(corrId, date);

        // When
        metricsRepository.recordRunCompleted(corrId, 7, 1500L);

        // Then
        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM aggregation_runs WHERE correlation_id = ?::uuid",
                String.class, corrId);
        assertThat(status).isEqualTo("SUCCESS");

        Integer written = jdbcTemplate.queryForObject(
                "SELECT records_written FROM aggregation_runs WHERE correlation_id = ?::uuid",
                Integer.class, corrId);
        assertThat(written).isEqualTo(7);
    }

    @Test
    @DisplayName("recordRunFailed updates status to FAILED with error message")
    void shouldRecordRunFailed() {
        // Given
        String corrId = "run-failed-" + System.nanoTime();
        LocalDate date = LocalDate.of(2024, 7, 1);
        metricsRepository.recordRunStarted(corrId, date);

        // When
        metricsRepository.recordRunFailed(corrId, "ClickHouse is down", 2000L);

        // Then
        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM aggregation_runs WHERE correlation_id = ?::uuid",
                String.class, corrId);
        assertThat(status).isEqualTo("FAILED");

        String errorMsg = jdbcTemplate.queryForObject(
                "SELECT error_message FROM aggregation_runs WHERE correlation_id = ?::uuid",
                String.class, corrId);
        assertThat(errorMsg).isEqualTo("ClickHouse is down");
    }

    // ─── s3_exports ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("recordS3Export inserts export record and is idempotent on same date")
    void shouldRecordS3ExportAndBeIdempotent() {
        // Given
        LocalDate exportDate = LocalDate.of(2024, 8, 1);
        String s3Path = "s3://movie-analytics/daily/2024-08-01/aggregates.csv";

        // When - first export
        Result<Void, AppError> firstResult = metricsRepository.recordS3Export(exportDate, s3Path);
        assertThat(firstResult.isSuccess()).isTrue();

        // When - second export (idempotent update)
        String updatedPath = "s3://movie-analytics/daily/2024-08-01/aggregates_v2.csv";
        Result<Void, AppError> secondResult = metricsRepository.recordS3Export(exportDate, updatedPath);
        assertThat(secondResult.isSuccess()).isTrue();

        // Then - only one row, updated path
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM s3_exports WHERE export_date = ?",
                Integer.class, exportDate);
        assertThat(count).isEqualTo(1);

        String storedPath = jdbcTemplate.queryForObject(
                "SELECT s3_path FROM s3_exports WHERE export_date = ?",
                String.class, exportDate);
        assertThat(storedPath).isEqualTo(updatedPath);
    }
}
