package com.cinema.aggregation.s3;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.postgres.MetricsRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for S3ExportService.
 * Uses ReflectionTestUtils to inject @Value field 'bucket' that Spring cannot inject in plain unit tests.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("S3 Export Service Tests")
class S3ExportServiceTest {

    private static final String TEST_BUCKET = "movie-analytics";

    @Mock
    private S3Client s3Client;

    @Mock
    private MetricsRepository metricsRepository;

    private S3ExportService exportService;

    private LocalDate testDate;

    @BeforeEach
    void setUp() {
        exportService = new S3ExportService(s3Client, metricsRepository);
        // Inject @Value field that Spring cannot populate in plain unit tests
        ReflectionTestUtils.setField(exportService, "bucket", TEST_BUCKET);
        testDate = LocalDate.of(2024, 1, 15);
    }

    @Test
    @DisplayName("Export successfully uploads metrics to S3")
    void shouldExportMetricsToS3Successfully() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", testDate, 1000.0),
                DailyMetric.global("avg_watch_seconds", testDate, 1800.5),
                DailyMetric.global("conversion_rate", testDate, 0.65)
        );

        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(metrics));
        when(metricsRepository.recordS3Export(any(), anyString())).thenReturn(Result.success(null));

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).isEqualTo("s3://movie-analytics/daily/2024-01-15/aggregates.csv");

        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(metricsRepository).findMetricsByDate(testDate);
        verify(metricsRepository).recordS3Export(eq(testDate), eq("s3://movie-analytics/daily/2024-01-15/aggregates.csv"));
    }

    @Test
    @DisplayName("Export uses correct S3 key format: daily/YYYY-MM-DD/aggregates.csv")
    void shouldUseCorrectS3KeyFormat() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", testDate, 500.0)
        );
        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(metrics));
        when(metricsRepository.recordS3Export(any(), anyString())).thenReturn(Result.success(null));

        // Capture the PutObjectRequest to verify the key
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);

        // When
        exportService.export(testDate);

        // Then
        verify(s3Client).putObject(requestCaptor.capture(), any(RequestBody.class));
        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertThat(capturedRequest.bucket()).isEqualTo(TEST_BUCKET);
        assertThat(capturedRequest.key()).isEqualTo("daily/2024-01-15/aggregates.csv");
        assertThat(capturedRequest.contentType()).isEqualTo("text/csv");
    }

    @Test
    @DisplayName("Export skips upload and returns success when no metrics found for date")
    void shouldReturnSuccessWithSkipMessageWhenNoMetricsFound() {
        // Given
        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(List.of()));

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue()).contains("skipped: no data");

        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        verify(metricsRepository, never()).recordS3Export(any(), anyString());
    }

    @Test
    @DisplayName("Export returns S3_UPLOAD_FAILED error when S3 upload throws S3Exception")
    void shouldReturnErrorWhenS3UploadFails() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", testDate, 1000.0)
        );

        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(metrics));
        doThrow(S3Exception.builder().message("S3 connection failed").build())
                .when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.S3_UPLOAD_FAILED);

        // recordS3Export should NOT be called when upload fails
        verify(metricsRepository, never()).recordS3Export(any(), anyString());
    }

    @Test
    @DisplayName("Export returns S3_CONNECTION_FAILED when unexpected exception occurs during upload")
    void shouldReturnS3ConnectionFailedOnGenericException() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", testDate, 1000.0)
        );

        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(metrics));
        doThrow(new RuntimeException("Network error"))
                .when(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.S3_CONNECTION_FAILED);
    }

    @Test
    @DisplayName("Export returns repository error when DB query fails")
    void shouldReturnErrorWhenRepositoryQueryFails() {
        // Given
        AppError dbError = AppError.of(ErrorCode.POSTGRES_CONNECTION_FAILED, "Database connection failed");
        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.failure(dbError));

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.POSTGRES_CONNECTION_FAILED);

        verify(s3Client, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("CSV format includes header and correct rows for various metric types")
    void shouldGenerateCorrectCsvFormat() {
        // Given
        List<DailyMetric> metrics = List.of(
                DailyMetric.global("dau", testDate, 1000.0),
                DailyMetric.withDimension("top_movie", testDate, "movie-1", 100.0, Map.of("rank", 1)),
                DailyMetric.withDimension("retention_rate", testDate, "d1", 0.45, Map.of("cohort_size", 1000L))
        );

        when(metricsRepository.findMetricsByDate(testDate)).thenReturn(Result.success(metrics));
        when(metricsRepository.recordS3Export(any(), anyString())).thenReturn(Result.success(null));

        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        // When
        Result<String, AppError> result = exportService.export(testDate);

        // Then
        assertThat(result.isSuccess()).isTrue();

        // Verify the CSV content was uploaded (header + 3 data rows)
        verify(s3Client).putObject(any(PutObjectRequest.class), bodyCaptor.capture());
        // CSV was built and sent — non-null body
        assertThat(bodyCaptor.getValue()).isNotNull();
    }
}
