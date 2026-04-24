package com.cinema.producer.kafka;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.avro.MovieEvent;
import com.cinema.producer.domain.MovieEventRequest;
import com.cinema.producer.domain.PublishResult;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import com.cinema.producer.error.Result;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaProducerService.
 * Tests publish() (with validation) and publishAvroEvent() (direct publish).
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Kafka Producer Service Tests")
class KafkaProducerServiceTest {

    private static final String TEST_TOPIC = "movie-events";
    private static final String CORRELATION_ID = "test-correlation-id";

    @Mock
    private KafkaTemplate<String, MovieEvent> kafkaTemplate;

    @Mock
    private EventValidator eventValidator;

    private KafkaProducerService kafkaProducerService;

    @BeforeEach
    void setUp() {
        // Build real EventValidator for validation path tests; use @Mock for exception path tests
        EventValidator realValidator = new EventValidator();
        kafkaProducerService = new KafkaProducerService(kafkaTemplate, realValidator);
        ReflectionTestUtils.setField(kafkaProducerService, "topic", TEST_TOPIC);
    }

    // ─── publish() with request validation ───────────────────────────────────

    @Test
    @DisplayName("publish() with valid request succeeds and returns PublishResult")
    void shouldPublishValidRequestSuccessfully() throws Exception {
        // Given
        MovieEventRequest request = new MovieEventRequest(
                null, "user-1", "movie-1", "VIEW_STARTED", "MOBILE", "session-1", 0
        );
        SendResult<String, MovieEvent> sendResult = mockSendResult(TEST_TOPIC, 0, 42L);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(completedFuture(sendResult));

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publish(request, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().topic()).isEqualTo(TEST_TOPIC);
        assertThat(result.getValue().partition()).isEqualTo(0);
        assertThat(result.getValue().offset()).isEqualTo(42L);
        assertThat(result.getValue().eventId()).isNotNull();
    }

    @Test
    @DisplayName("publish() uses provided eventId when present in request")
    void shouldUseProvidedEventIdFromRequest() throws Exception {
        // Given
        String customEventId = "custom-event-id-123";
        MovieEventRequest request = new MovieEventRequest(
                customEventId, "user-1", "movie-1", "LIKED", "DESKTOP", "session-1", 0
        );
        SendResult<String, MovieEvent> sendResult = mockSendResult(TEST_TOPIC, 1, 100L);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(completedFuture(sendResult));

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publish(request, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().eventId()).isEqualTo(customEventId);
    }

    @Test
    @DisplayName("publish() fails with INVALID_EVENT_TYPE for unknown eventType string")
    void shouldFailWithInvalidEventTypeError() {
        // Given
        MovieEventRequest request = new MovieEventRequest(
                null, "user-1", "movie-1", "INVALID_TYPE", "MOBILE", "session-1", 0
        );

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publish(request, CORRELATION_ID);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.INVALID_EVENT_TYPE);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("publish() fails with INVALID_DEVICE_TYPE for unknown deviceType string")
    void shouldFailWithInvalidDeviceTypeError() {
        // Given
        MovieEventRequest request = new MovieEventRequest(
                null, "user-1", "movie-1", "VIEW_STARTED", "SMART_TV", "session-1", 0
        );

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publish(request, CORRELATION_ID);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.INVALID_DEVICE_TYPE);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("publish() uses userId as Kafka partition key")
    void shouldUseUserIdAsPartitionKey() throws Exception {
        // Given
        String userId = "user-42";
        MovieEventRequest request = new MovieEventRequest(
                null, userId, "movie-1", "VIEW_STARTED", "TV", "session-1", 0
        );
        SendResult<String, MovieEvent> sendResult = mockSendResult(TEST_TOPIC, 0, 1L);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(completedFuture(sendResult));

        ArgumentCaptor<ProducerRecord<String, MovieEvent>> captor = ArgumentCaptor.forClass(ProducerRecord.class);

        // When
        kafkaProducerService.publish(request, CORRELATION_ID);

        // Then
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().key()).isEqualTo(userId);
        assertThat(captor.getValue().topic()).isEqualTo(TEST_TOPIC);
    }

    // ─── publishAvroEvent() direct publish ────────────────────────────────────

    @Test
    @DisplayName("publishAvroEvent() succeeds when Kafka send completes normally")
    void shouldPublishAvroEventSuccessfully() throws Exception {
        // Given
        MovieEvent event = buildTestEvent("user-1", "movie-1", EventType.VIEW_FINISHED);
        SendResult<String, MovieEvent> sendResult = mockSendResult(TEST_TOPIC, 2, 99L);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(completedFuture(sendResult));

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publishAvroEvent(event, CORRELATION_ID);

        // Then
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getValue().topic()).isEqualTo(TEST_TOPIC);
        assertThat(result.getValue().partition()).isEqualTo(2);
        assertThat(result.getValue().offset()).isEqualTo(99L);
    }

    @Test
    @DisplayName("publishAvroEvent() returns KAFKA_PUBLISH_FAILED on ExecutionException")
    void shouldReturnErrorOnExecutionException() throws Exception {
        // Given
        MovieEvent event = buildTestEvent("user-1", "movie-1", EventType.VIEW_STARTED);
        CompletableFuture<SendResult<String, MovieEvent>> future = new CompletableFuture<>();
        future.completeExceptionally(new ExecutionException("Broker unavailable", new RuntimeException("Connection refused")));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publishAvroEvent(event, CORRELATION_ID);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.KAFKA_PUBLISH_FAILED);
    }

    @Test
    @DisplayName("publishAvroEvent() returns KAFKA_PUBLISH_FAILED on TimeoutException")
    void shouldReturnErrorOnTimeoutException() throws Exception {
        // Given
        MovieEvent event = buildTestEvent("user-1", "movie-1", EventType.VIEW_PAUSED);
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, MovieEvent>> future = mock(CompletableFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new TimeoutException("Kafka timeout"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publishAvroEvent(event, CORRELATION_ID);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.KAFKA_PUBLISH_FAILED);
        assertThat(result.getError().message()).contains("Timeout");
    }

    @Test
    @DisplayName("publishAvroEvent() returns KAFKA_PUBLISH_FAILED on InterruptedException")
    void shouldReturnErrorOnInterruptedException() throws Exception {
        // Given
        MovieEvent event = buildTestEvent("user-1", "movie-1", EventType.VIEW_RESUMED);
        @SuppressWarnings("unchecked")
        CompletableFuture<SendResult<String, MovieEvent>> future = mock(CompletableFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException("Thread interrupted"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

        // When
        Result<PublishResult, AppError> result = kafkaProducerService.publishAvroEvent(event, CORRELATION_ID);

        // Then
        assertThat(result.isFailure()).isTrue();
        assertThat(result.getError().code()).isEqualTo(ErrorCode.KAFKA_PUBLISH_FAILED);
        // Thread interrupted flag should be restored
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        Thread.interrupted(); // clear flag for next tests
    }

    // ─── Helper methods ───────────────────────────────────────────────────────

    private MovieEvent buildTestEvent(String userId, String movieId, EventType eventType) {
        return MovieEvent.newBuilder()
                .setEventId("test-event-id-" + System.nanoTime())
                .setUserId(userId)
                .setMovieId(movieId)
                .setEventType(eventType)
                .setTimestamp(Instant.now())
                .setDeviceType(DeviceType.MOBILE)
                .setSessionId("test-session")
                .setProgressSeconds(0)
                .build();
    }

    @SuppressWarnings("unchecked")
    private SendResult<String, MovieEvent> mockSendResult(String topic, int partition, long offset) {
        SendResult<String, MovieEvent> sendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(topic, partition), offset, 0, System.currentTimeMillis(), 0, 0
        );
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        return sendResult;
    }

    private <T> CompletableFuture<T> completedFuture(T value) {
        return CompletableFuture.completedFuture(value);
    }
}
