package com.cinema.producer.kafka;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.avro.MovieEvent;
import com.cinema.producer.domain.MovieEventRequest;
import com.cinema.producer.domain.PublishResult;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import com.cinema.producer.error.Result;
import com.cinema.producer.logging.EventLogger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Сервис публикации событий в Kafka.
 *
 * <p>Валидирует событие, сериализует в Avro и публикует с ключом user_id
 * (для сохранения порядка событий одного пользователя в одной партиции).
 *
 * <p>Обработка ошибок:
 * <ul>
 *   <li>Ошибки валидации → {@link Result#failure} с кодом VALIDATION_FAILED</li>
 *   <li>Ошибки Kafka → {@link Result#failure} с кодом KAFKA_PUBLISH_FAILED</li>
 * </ul>
 *
 * <p>Retry с exponential backoff настроен на уровне KafkaProducer (см. application.yml).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private static final int PUBLISH_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, MovieEvent> kafkaTemplate;
    private final EventValidator eventValidator;

    @Value("${cinema.kafka.topic}")
    private String topic;

    /**
     * Публикует событие в Kafka. Возвращает Result — никогда не бросает исключений
     * для бизнес-сценариев (согласно rules.md).
     *
     * @param request входящий запрос события
     * @param correlationId ID корреляции для трассировки
     * @return Result с PublishResult при успехе или AppError при ошибке
     */
    public Result<PublishResult, AppError> publish(MovieEventRequest request, String correlationId) {
        Result<MovieEvent, AppError> validationResult = buildAndValidateEvent(request, correlationId);
        if (validationResult.isFailure()) {
            return Result.failure(validationResult.getError());
        }

        MovieEvent event = validationResult.getValue();
        return doPublish(event, correlationId);
    }

    /**
     * Публикует уже сформированное Avro-событие (для генератора).
     */
    public Result<PublishResult, AppError> publishAvroEvent(MovieEvent event, String correlationId) {
        return doPublish(event, correlationId);
    }

    private Result<MovieEvent, AppError> buildAndValidateEvent(MovieEventRequest request, String correlationId) {
        Result<EventType, AppError> eventTypeResult = eventValidator.parseEventType(request.eventType());
        if (eventTypeResult.isFailure()) {
            EventLogger.logValidationError(correlationId, "event_type", request.eventType());
            return Result.failure(eventTypeResult.getError());
        }

        Result<DeviceType, AppError> deviceTypeResult = eventValidator.parseDeviceType(request.deviceType());
        if (deviceTypeResult.isFailure()) {
            EventLogger.logValidationError(correlationId, "device_type", request.deviceType());
            return Result.failure(deviceTypeResult.getError());
        }

        String eventId = request.eventId() != null ? request.eventId() : UUID.randomUUID().toString();

        MovieEvent event = MovieEvent.newBuilder()
                .setEventId(eventId)
                .setUserId(request.userId())
                .setMovieId(request.movieId())
                .setEventType(eventTypeResult.getValue())
                .setTimestamp(Instant.now())  // Avro timestamp-millis logical type → Instant
                .setDeviceType(deviceTypeResult.getValue())
                .setSessionId(request.sessionId())
                .setProgressSeconds(request.progressSeconds())
                .build();

        EventLogger.logEventReceived(correlationId, eventId, request.eventType());
        return Result.success(event);
    }

    private Result<PublishResult, AppError> doPublish(MovieEvent event, String correlationId) {
        // Ключ партиционирования — user_id для сохранения порядка событий одного пользователя
        ProducerRecord<String, MovieEvent> record = new ProducerRecord<>(
                topic,
                event.getUserId().toString(),
                event
        );

        try {
            SendResult<String, MovieEvent> sendResult = kafkaTemplate.send(record)
                    .get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            RecordMetadata metadata = sendResult.getRecordMetadata();
            PublishResult publishResult = new PublishResult(
                    event.getEventId().toString(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset()
            );

            EventLogger.logPublishSuccess(
                    correlationId,
                    publishResult.eventId(),
                    event.getEventType().toString(),
                    event.getUserId().toString(),
                    publishResult.topic(),
                    publishResult.partition(),
                    publishResult.offset()
            );

            return Result.success(publishResult);

        } catch (ExecutionException e) {
            String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            EventLogger.logPublishFailure(correlationId, event.getEventId().toString(),
                    event.getEventType().toString(), ErrorCode.KAFKA_PUBLISH_FAILED.getCode(), errorMsg);
            return Result.failure(AppError.of(ErrorCode.KAFKA_PUBLISH_FAILED, errorMsg));

        } catch (TimeoutException e) {
            EventLogger.logPublishFailure(correlationId, event.getEventId().toString(),
                    event.getEventType().toString(), ErrorCode.KAFKA_PUBLISH_FAILED.getCode(),
                    "Timeout после " + PUBLISH_TIMEOUT_SECONDS + "s");
            return Result.failure(AppError.of(ErrorCode.KAFKA_PUBLISH_FAILED,
                    "Timeout публикации в Kafka после " + PUBLISH_TIMEOUT_SECONDS + " секунд"));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.failure(AppError.of(ErrorCode.KAFKA_PUBLISH_FAILED, "Публикация прервана"));
        }
    }
}
