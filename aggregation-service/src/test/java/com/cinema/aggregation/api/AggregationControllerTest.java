package com.cinema.aggregation.api;

import com.cinema.aggregation.domain.AggregationService;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.s3.S3ExportService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDate;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * MockMvc slice tests for AggregationController.
 */
@WebMvcTest(AggregationController.class)
@DisplayName("Aggregation Controller Tests")
class AggregationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private AggregationService aggregationService;

    @MockBean
    private S3ExportService s3ExportService;

    // ─── GET /api/v1/aggregation/health ──────────────────────────────────────

    @Test
    @DisplayName("GET /health returns 200 UP")
    void shouldReturnHealthUp() throws Exception {
        mockMvc.perform(get("/api/v1/aggregation/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("UP"));
    }

    // ─── POST /api/v1/aggregation/run ─────────────────────────────────────────

    @Test
    @DisplayName("POST /run without date parameter uses yesterday and returns 200")
    void shouldRunAggregationWithDefaultDate() throws Exception {
        // Given
        when(aggregationService.aggregate(any(LocalDate.class))).thenReturn(Result.success(7));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/run"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.records_written").value(7))
                .andExpect(jsonPath("$.target_date").exists());
    }

    @Test
    @DisplayName("POST /run?date=2024-01-15 uses specified date and returns 200")
    void shouldRunAggregationWithSpecificDate() throws Exception {
        // Given
        when(aggregationService.aggregate(LocalDate.of(2024, 1, 15))).thenReturn(Result.success(5));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/run").param("date", "2024-01-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.target_date").value("2024-01-15"))
                .andExpect(jsonPath("$.records_written").value(5));
    }

    @Test
    @DisplayName("POST /run returns 500 when aggregation fails")
    void shouldReturn500WhenAggregationFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.AGGREGATION_FAILED, "All ClickHouse queries failed");
        when(aggregationService.aggregate(any(LocalDate.class))).thenReturn(Result.failure(error));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/run"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("AGGREGATION_FAILED"));
    }

    @Test
    @DisplayName("POST /run returns 500 when ClickHouse is unavailable")
    void shouldReturn500WhenClickHouseFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.CLICKHOUSE_QUERY_FAILED, "ClickHouse connection refused");
        when(aggregationService.aggregate(any(LocalDate.class))).thenReturn(Result.failure(error));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/run"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("CLICKHOUSE_QUERY_FAILED"));
    }

    // ─── POST /api/v1/aggregation/export ─────────────────────────────────────

    @Test
    @DisplayName("POST /export without date uses yesterday and returns 200 with s3_path")
    void shouldExportWithDefaultDate() throws Exception {
        // Given
        String s3Path = "s3://movie-analytics/daily/2024-01-14/aggregates.csv";
        when(s3ExportService.export(any(LocalDate.class))).thenReturn(Result.success(s3Path));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/export"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.s3_path").value(s3Path))
                .andExpect(jsonPath("$.export_date").exists());
    }

    @Test
    @DisplayName("POST /export?date=2024-01-15 uses specified date and returns 200")
    void shouldExportWithSpecificDate() throws Exception {
        // Given
        String s3Path = "s3://movie-analytics/daily/2024-01-15/aggregates.csv";
        when(s3ExportService.export(LocalDate.of(2024, 1, 15))).thenReturn(Result.success(s3Path));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/export").param("date", "2024-01-15"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.export_date").value("2024-01-15"))
                .andExpect(jsonPath("$.s3_path").value(s3Path));
    }

    @Test
    @DisplayName("POST /export returns 503 when S3 connection fails")
    void shouldReturn503WhenS3ConnectionFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.S3_CONNECTION_FAILED, "MinIO unavailable");
        when(s3ExportService.export(any(LocalDate.class))).thenReturn(Result.failure(error));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/export"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("S3_CONNECTION_FAILED"));
    }

    @Test
    @DisplayName("POST /export returns 500 when S3 upload fails (not connection)")
    void shouldReturn500WhenS3UploadFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.S3_UPLOAD_FAILED, "Upload failed");
        when(s3ExportService.export(any(LocalDate.class))).thenReturn(Result.failure(error));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/export"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("S3_UPLOAD_FAILED"));
    }

    @Test
    @DisplayName("POST /export returns 500 when PostgreSQL read fails")
    void shouldReturn500WhenPostgresReadFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.POSTGRES_CONNECTION_FAILED, "DB unavailable");
        when(s3ExportService.export(any(LocalDate.class))).thenReturn(Result.failure(error));

        // When / Then
        mockMvc.perform(post("/api/v1/aggregation/export"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("POSTGRES_CONNECTION_FAILED"));
    }
}
