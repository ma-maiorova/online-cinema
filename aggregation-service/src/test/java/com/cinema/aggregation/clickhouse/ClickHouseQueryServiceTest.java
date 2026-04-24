package com.cinema.aggregation.clickhouse;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ClickHouseQueryService using mocked raw JDBC (DataSource → Connection → PreparedStatement → ResultSet).
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ClickHouse Query Service Tests")
class ClickHouseQueryServiceTest {

    @Mock
    private DataSource clickHouseDataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    private ClickHouseQueryService queryService;

    private LocalDate testDate;

    @BeforeEach
    void setUp() throws SQLException {
        queryService = new ClickHouseQueryService(clickHouseDataSource);
        testDate = LocalDate.of(2024, 1, 15);

        when(clickHouseDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    // ─── DAU ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("DAU computation returns correct unique user count")
    void shouldComputeDauCorrectly() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("dau")).thenReturn(1000L);

        // When
        Result<DailyMetric, AppError> result = queryService.computeDau(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().metricValue()).isEqualTo(1000.0);
        assertThat(result.getValue().metricName()).isEqualTo("dau");
        assertThat(result.getValue().dimensionKey()).isEqualTo("global");
        assertThat(result.getValue().metricDate()).isEqualTo(testDate);
    }

    @Test
    @DisplayName("DAU computation returns error when result set is empty")
    void shouldReturnErrorWhenDauResultSetEmpty() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(false);

        // When
        Result<DailyMetric, AppError> result = queryService.computeDau(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }

    @Test
    @DisplayName("DAU computation returns error on SQL exception")
    void shouldReturnErrorOnDauSqlException() throws SQLException {
        // Given
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("ClickHouse connection failed"));

        // When
        Result<DailyMetric, AppError> result = queryService.computeDau(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }

    // ─── Average Watch Time ───────────────────────────────────────────────────

    @Test
    @DisplayName("Average watch time computation returns correct value")
    void shouldComputeAvgWatchTimeCorrectly() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getDouble("avg_watch_seconds")).thenReturn(1800.5);

        // When
        Result<DailyMetric, AppError> result = queryService.computeAvgWatchTime(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().metricValue()).isEqualTo(1800.5);
        assertThat(result.getValue().metricName()).isEqualTo("avg_watch_seconds");
        assertThat(result.getValue().dimensionKey()).isEqualTo("global");
    }

    @Test
    @DisplayName("Average watch time returns error on SQL exception")
    void shouldReturnErrorOnAvgWatchTimeSqlException() throws SQLException {
        // Given
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("Query timeout"));

        // When
        Result<DailyMetric, AppError> result = queryService.computeAvgWatchTime(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }

    // ─── Conversion Rate ──────────────────────────────────────────────────────

    @Test
    @DisplayName("Conversion rate computation returns correct percentage")
    void shouldComputeConversionRateCorrectly() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getDouble("conversion_rate")).thenReturn(0.65);

        // When
        Result<DailyMetric, AppError> result = queryService.computeConversionRate(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().metricValue()).isEqualTo(0.65);
        assertThat(result.getValue().metricName()).isEqualTo("conversion_rate");
    }

    @Test
    @DisplayName("Conversion rate returns error on SQL exception")
    void shouldReturnErrorOnConversionRateSqlException() throws SQLException {
        // Given
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("ClickHouse unavailable"));

        // When
        Result<DailyMetric, AppError> result = queryService.computeConversionRate(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }

    // ─── Top Movies ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("Top movies computation returns ranked list")
    void shouldComputeTopMoviesCorrectly() throws SQLException {
        // Given - simulate 3 rows
        when(resultSet.next()).thenReturn(true, true, true, false);
        when(resultSet.getString("movie_id")).thenReturn("movie-1", "movie-2", "movie-3");
        when(resultSet.getLong("view_count")).thenReturn(100L, 80L, 60L);
        when(resultSet.getInt("rank")).thenReturn(1, 2, 3);

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeTopMovies(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        List<DailyMetric> metrics = result.getValue();
        assertThat(metrics).hasSize(3);

        assertThat(metrics.get(0).metricName()).isEqualTo("top_movie");
        assertThat(metrics.get(0).dimensionKey()).isEqualTo("movie-1");
        assertThat(metrics.get(0).metricValue()).isEqualTo(100.0);
        assertThat(metrics.get(0).extraData()).containsEntry("rank", 1);
        assertThat(metrics.get(0).extraData()).containsEntry("view_count", 100L);

        assertThat(metrics.get(1).dimensionKey()).isEqualTo("movie-2");
        assertThat(metrics.get(1).extraData()).containsEntry("rank", 2);

        assertThat(metrics.get(2).dimensionKey()).isEqualTo("movie-3");
        assertThat(metrics.get(2).extraData()).containsEntry("rank", 3);
    }

    @Test
    @DisplayName("Top movies returns empty list when no data")
    void shouldReturnEmptyTopMoviesWhenNoData() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(false);

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeTopMovies(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    @DisplayName("Top movies returns error on SQL exception")
    void shouldReturnErrorOnTopMoviesSqlException() throws SQLException {
        // Given
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("ClickHouse connection failed"));

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeTopMovies(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }

    // ─── Retention ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("Retention computation returns D1 and D7 metrics")
    void shouldComputeRetentionCorrectly() throws SQLException {
        // Given - single result row with cohort_size, d1_count, d7_count
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("cohort_size")).thenReturn(1000L);
        when(resultSet.getLong("d1_count")).thenReturn(450L);
        when(resultSet.getLong("d7_count")).thenReturn(300L);

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeRetention(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        List<DailyMetric> metrics = result.getValue();
        assertThat(metrics).hasSize(2);

        DailyMetric d1 = metrics.get(0);
        assertThat(d1.metricName()).isEqualTo("retention_rate");
        assertThat(d1.dimensionKey()).isEqualTo("d1");
        assertThat(d1.metricValue()).isEqualTo(0.45);
        assertThat(d1.extraData()).containsEntry("cohort_size", 1000L);
        assertThat(d1.extraData()).containsEntry("retained_count", 450L);
        assertThat(d1.extraData()).containsEntry("day_number", 1);

        DailyMetric d7 = metrics.get(1);
        assertThat(d7.metricName()).isEqualTo("retention_rate");
        assertThat(d7.dimensionKey()).isEqualTo("d7");
        assertThat(d7.metricValue()).isEqualTo(0.30);
        assertThat(d7.extraData()).containsEntry("cohort_size", 1000L);
        assertThat(d7.extraData()).containsEntry("retained_count", 300L);
        assertThat(d7.extraData()).containsEntry("day_number", 7);
    }

    @Test
    @DisplayName("Retention returns empty list when result set is empty")
    void shouldReturnEmptyRetentionWhenNoData() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(false);

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeRetention(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEmpty();
    }

    @Test
    @DisplayName("Retention returns zero rates when cohort size is zero")
    void shouldReturnZeroRatesWhenCohortSizeIsZero() throws SQLException {
        // Given
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("cohort_size")).thenReturn(0L);
        when(resultSet.getLong("d1_count")).thenReturn(0L);
        when(resultSet.getLong("d7_count")).thenReturn(0L);

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeRetention(testDate, "test-correlation");

        // Then
        assertThat(result.isSuccess()).isTrue();
        List<DailyMetric> metrics = result.getValue();
        assertThat(metrics).hasSize(2);
        assertThat(metrics.get(0).metricValue()).isEqualTo(0.0);
        assertThat(metrics.get(1).metricValue()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Retention returns error on SQL exception")
    void shouldReturnErrorOnRetentionSqlException() throws SQLException {
        // Given
        when(preparedStatement.executeQuery()).thenThrow(new SQLException("ClickHouse timeout"));

        // When
        Result<List<DailyMetric>, AppError> result = queryService.computeRetention(testDate, "test-correlation");

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.CLICKHOUSE_QUERY_FAILED);
    }
}
