package com.cinema.producer.api;

import com.cinema.producer.domain.PublishResult;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import com.cinema.producer.error.Result;
import com.cinema.producer.kafka.KafkaProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * MockMvc slice tests for EventController.
 * Only loads the web layer — Kafka and other beans are mocked.
 */
@WebMvcTest(EventController.class)
@DisplayName("Event Controller Tests")
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaProducerService kafkaProducerService;

    // ─── GET /api/v1/events/health ────────────────────────────────────────────

    @Test
    @DisplayName("GET /health returns 200 UP")
    void shouldReturnHealthUp() throws Exception {
        mockMvc.perform(get("/api/v1/events/health"))
                .andExpect(status().isOk())
                .andExpect(content().string("UP"));
    }

    // ─── POST /api/v1/events — success cases ─────────────────────────────────

    @Test
    @DisplayName("POST /events with valid body returns 200 with PublishResult")
    void shouldReturn200OnValidRequest() throws Exception {
        // Given
        PublishResult publishResult = new PublishResult("evt-123", "movie-events", 0, 42L);
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.success(publishResult));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        // When / Then
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value("evt-123"))
                .andExpect(jsonPath("$.topic").value("movie-events"))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(42));
    }

    @Test
    @DisplayName("POST /events with X-Correlation-Id header uses provided correlationId")
    void shouldUseProvidedCorrelationIdHeader() throws Exception {
        // Given
        PublishResult publishResult = new PublishResult("evt-456", "movie-events", 1, 10L);
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.success(publishResult));

        String body = """
                {
                    "userId": "user-2",
                    "movieId": "movie-2",
                    "eventType": "LIKED",
                    "deviceType": "TV",
                    "sessionId": "session-2",
                    "progressSeconds": 0
                }
                """;

        // When / Then
        mockMvc.perform(post("/api/v1/events")
                        .header("X-Correlation-Id", "my-correlation-id")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value("evt-456"));
    }

    // ─── POST /api/v1/events — validation errors ─────────────────────────────

    @Test
    @DisplayName("POST /events with missing userId returns 400")
    void shouldReturn400WhenUserIdMissing() throws Exception {
        String body = """
                {
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /events with blank userId returns 400")
    void shouldReturn400WhenUserIdIsBlank() throws Exception {
        String body = """
                {
                    "userId": "",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /events with missing movieId returns 400")
    void shouldReturn400WhenMovieIdMissing() throws Exception {
        String body = """
                {
                    "userId": "user-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /events with missing sessionId returns 400")
    void shouldReturn400WhenSessionIdMissing() throws Exception {
        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /events with invalid eventType returns 400 with INVALID_EVENT_TYPE code")
    void shouldReturn400WithInvalidEventTypeError() throws Exception {
        // Given - service returns INVALID_EVENT_TYPE error
        AppError error = AppError.of(ErrorCode.INVALID_EVENT_TYPE, "Unknown event_type: 'INVALID'");
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.failure(error));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "INVALID",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_EVENT_TYPE"));
    }

    @Test
    @DisplayName("POST /events with invalid deviceType returns 400 with INVALID_DEVICE_TYPE code")
    void shouldReturn400WithInvalidDeviceTypeError() throws Exception {
        // Given - service returns INVALID_DEVICE_TYPE error
        AppError error = AppError.of(ErrorCode.INVALID_DEVICE_TYPE, "Unknown device_type: 'INVALID'");
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.failure(error));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "INVALID_DEVICE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_DEVICE_TYPE"));
    }

    // ─── POST /api/v1/events — Kafka unavailable ──────────────────────────────

    @Test
    @DisplayName("POST /events returns 503 when Kafka publish fails")
    void shouldReturn503WhenKafkaFails() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.KAFKA_PUBLISH_FAILED, "Broker unavailable");
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.failure(error));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "MOBILE",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("KAFKA_PUBLISH_FAILED"));
    }

    @Test
    @DisplayName("POST /events returns 503 when Schema Registry unavailable")
    void shouldReturn503WhenSchemaRegistryUnavailable() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.SCHEMA_REGISTRY_UNAVAILABLE, "Schema Registry down");
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.failure(error));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "DESKTOP",
                    "sessionId": "session-1",
                    "progressSeconds": 100
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("SCHEMA_REGISTRY_UNAVAILABLE"));
    }

    @Test
    @DisplayName("POST /events returns 500 on internal error")
    void shouldReturn500OnInternalError() throws Exception {
        // Given
        AppError error = AppError.of(ErrorCode.INTERNAL_ERROR, "Unexpected error");
        when(kafkaProducerService.publish(any(), nullable(String.class))).thenReturn(Result.failure(error));

        String body = """
                {
                    "userId": "user-1",
                    "movieId": "movie-1",
                    "eventType": "VIEW_STARTED",
                    "deviceType": "TABLET",
                    "sessionId": "session-1",
                    "progressSeconds": 0
                }
                """;

        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"));
    }
}
