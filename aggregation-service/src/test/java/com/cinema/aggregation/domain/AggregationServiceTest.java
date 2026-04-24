package com.cinema.aggregation.domain;

import com.cinema.aggregation.clickhouse.ClickHouseQueryService;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.postgres.MetricsRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AggregationService.
 *
 * Key design rule (see AggregationService.java):
 * - Service uses PARTIAL FAILURE TOLERANCE — if some metrics fail, it continues with others.
 * - Only fails entirely if ALL metrics fail (allMetrics.isEmpty()).
 * - recordRunStarted is always called at the start.
 * - recordRunCompleted / recordRunFailed is called based on the save result.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Aggregation Service Tests")
class AggregationServiceTest {

    @Mock
    private ClickHouseQueryService clickHouseQueryService;

    @Mock
    private MetricsRepository metricsRepository;

    @InjectMocks
    private AggregationService aggregationService;

    private LocalDate testDate;

    @BeforeEach
    void setUp() {
        testDate = LocalDate.of(2024, 1, 15);
        // recordRunStarted and recordRunCompleted/Failed are always called — set up defaults
        when(metricsRepository.recordRunStarted(any(), eq(testDate))).thenReturn(Result.success(null));
    }

    @Test
    @DisplayName("Aggregation successfully computes and saves all 7 metrics")
    void shouldAggregateSuccessfully() {
        // Given
        when(clickHouseQueryService.computeDau(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("dau", testDate, 1000.0)));

        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("avg_watch_seconds", testDate, 1800.5)));

        when(clickHouseQueryService.computeConversionRate(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("conversion_rate", testDate, 0.65)));

        when(clickHouseQueryService.computeTopMovies(eq(testDate), any()))
                .thenReturn(Result.success(List.of(
                        DailyMetric.withDimension("top_movie", testDate, "movie-1", 100.0, null),
                        DailyMetric.withDimension("top_movie", testDate, "movie-2", 80.0, null)
                )));

        when(clickHouseQueryService.computeRetention(eq(testDate), any()))
                .thenReturn(Result.success(List.of(
                        DailyMetric.withDimension("retention_rate", testDate, "d1", 0.45, null),
                        DailyMetric.withDimension("retention_rate", testDate, "d7", 0.30, null)
                )));

        when(metricsRepository.saveMetrics(any(), any()))
                .thenReturn(Result.success(7));

        doNothing().when(metricsRepository).recordRunCompleted(any(), eq(7), anyLong());

        // When
        Result<Integer, AppError> result = aggregationService.aggregate(testDate);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(7);

        verify(clickHouseQueryService).computeDau(eq(testDate), any());
        verify(clickHouseQueryService).computeAvgWatchTime(eq(testDate), any());
        verify(clickHouseQueryService).computeConversionRate(eq(testDate), any());
        verify(clickHouseQueryService).computeTopMovies(eq(testDate), any());
        verify(clickHouseQueryService).computeRetention(eq(testDate), any());
        verify(metricsRepository).saveMetrics(any(), any());
        verify(metricsRepository).recordRunStarted(any(), eq(testDate));
        verify(metricsRepository).recordRunCompleted(any(), eq(7), anyLong());
    }

    @Test
    @DisplayName("Aggregation fails with AGGREGATION_FAILED when ALL ClickHouse queries fail")
    void shouldFailWhenAllClickHouseQueriesFail() {
        // Given - ALL metrics fail → allMetrics will be empty → AGGREGATION_FAILED
        AppError chError = AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, "Query failed");

        when(clickHouseQueryService.computeDau(eq(testDate), any()))
                .thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any()))
                .thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeConversionRate(eq(testDate), any()))
                .thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeTopMovies(eq(testDate), any()))
                .thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeRetention(eq(testDate), any()))
                .thenReturn(Result.failure(chError));

        doNothing().when(metricsRepository).recordRunFailed(any(), anyString(), anyLong());

        // When
        Result<Integer, AppError> result = aggregationService.aggregate(testDate);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.AGGREGATION_FAILED);

        // All 5 metrics are attempted (partial tolerance — all are tried)
        verify(clickHouseQueryService).computeDau(eq(testDate), any());
        verify(clickHouseQueryService).computeAvgWatchTime(eq(testDate), any());
        verify(clickHouseQueryService).computeConversionRate(eq(testDate), any());
        verify(clickHouseQueryService).computeTopMovies(eq(testDate), any());
        verify(clickHouseQueryService).computeRetention(eq(testDate), any());

        // saveMetrics is NOT called when all metrics fail
        verify(metricsRepository, never()).saveMetrics(any(), any());
        // recordRunFailed IS called
        verify(metricsRepository).recordRunFailed(any(), anyString(), anyLong());
    }

    @Test
    @DisplayName("Aggregation fails when PostgreSQL write fails after successful ClickHouse queries")
    void shouldFailWhenPostgresWriteFails() {
        // Given
        when(clickHouseQueryService.computeDau(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("dau", testDate, 1000.0)));

        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("avg_watch_seconds", testDate, 1800.5)));

        when(clickHouseQueryService.computeConversionRate(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("conversion_rate", testDate, 0.65)));

        when(clickHouseQueryService.computeTopMovies(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));

        when(clickHouseQueryService.computeRetention(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));

        when(metricsRepository.saveMetrics(any(), any()))
                .thenReturn(Result.failure(AppError.of(ErrorCode.POSTGRES_WRITE_FAILED, "Write failed")));

        doNothing().when(metricsRepository).recordRunFailed(any(), anyString(), anyLong());

        // When
        Result<Integer, AppError> result = aggregationService.aggregate(testDate);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.POSTGRES_WRITE_FAILED);

        verify(metricsRepository).saveMetrics(any(), any());
        verify(metricsRepository).recordRunFailed(any(), anyString(), anyLong());
        verify(metricsRepository, never()).recordRunCompleted(any(), anyInt(), anyLong());
    }

    @Test
    @DisplayName("Aggregation continues and succeeds when one metric computation fails (partial tolerance)")
    void shouldContinueWhenOneMetricFails() {
        // Given - avg_watch_time fails, but others succeed
        when(clickHouseQueryService.computeDau(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("dau", testDate, 1000.0)));

        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any()))
                .thenReturn(Result.failure(AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, "Query failed")));

        when(clickHouseQueryService.computeConversionRate(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("conversion_rate", testDate, 0.65)));

        when(clickHouseQueryService.computeTopMovies(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));

        when(clickHouseQueryService.computeRetention(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));

        when(metricsRepository.saveMetrics(any(), any()))
                .thenReturn(Result.success(2));

        doNothing().when(metricsRepository).recordRunCompleted(any(), eq(2), anyLong());

        // When
        Result<Integer, AppError> result = aggregationService.aggregate(testDate);

        // Then - partial success: 2 metrics saved despite one failure
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo(2);

        // All 5 metric computations are still attempted
        verify(clickHouseQueryService).computeDau(eq(testDate), any());
        verify(clickHouseQueryService).computeAvgWatchTime(eq(testDate), any());
        verify(clickHouseQueryService).computeConversionRate(eq(testDate), any());
        verify(clickHouseQueryService).computeTopMovies(eq(testDate), any());
        verify(clickHouseQueryService).computeRetention(eq(testDate), any());

        // saveMetrics IS called with the partial result
        verify(metricsRepository).saveMetrics(any(), any());
        verify(metricsRepository).recordRunCompleted(any(), eq(2), anyLong());
    }

    @Test
    @DisplayName("Aggregation records run lifecycle: started then completed on success")
    void shouldRecordRunLifecycleOnSuccess() {
        // Given
        when(clickHouseQueryService.computeDau(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("dau", testDate, 500.0)));
        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("avg_watch_seconds", testDate, 900.0)));
        when(clickHouseQueryService.computeConversionRate(eq(testDate), any()))
                .thenReturn(Result.success(DailyMetric.global("conversion_rate", testDate, 0.5)));
        when(clickHouseQueryService.computeTopMovies(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));
        when(clickHouseQueryService.computeRetention(eq(testDate), any()))
                .thenReturn(Result.success(List.of()));
        when(metricsRepository.saveMetrics(any(), any())).thenReturn(Result.success(3));
        doNothing().when(metricsRepository).recordRunCompleted(any(), eq(3), anyLong());

        // When
        aggregationService.aggregate(testDate);

        // Then - run lifecycle methods called in correct order
        verify(metricsRepository).recordRunStarted(any(), eq(testDate));
        verify(metricsRepository).recordRunCompleted(any(), eq(3), anyLong());
        verify(metricsRepository, never()).recordRunFailed(any(), anyString(), anyLong());
    }

    @Test
    @DisplayName("Aggregation records run lifecycle: started then failed when all metrics fail")
    void shouldRecordRunLifecycleOnFailure() {
        // Given - all fail
        AppError chError = AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, "fail");
        when(clickHouseQueryService.computeDau(eq(testDate), any())).thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeAvgWatchTime(eq(testDate), any())).thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeConversionRate(eq(testDate), any())).thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeTopMovies(eq(testDate), any())).thenReturn(Result.failure(chError));
        when(clickHouseQueryService.computeRetention(eq(testDate), any())).thenReturn(Result.failure(chError));
        doNothing().when(metricsRepository).recordRunFailed(any(), anyString(), anyLong());

        // When
        aggregationService.aggregate(testDate);

        // Then
        verify(metricsRepository).recordRunStarted(any(), eq(testDate));
        verify(metricsRepository).recordRunFailed(any(), anyString(), anyLong());
        verify(metricsRepository, never()).recordRunCompleted(any(), anyInt(), anyLong());
    }
}
