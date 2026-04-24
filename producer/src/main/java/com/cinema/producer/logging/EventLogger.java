package com.cinema.producer.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.UUID;

/**
 * Утилита для структурированного логирования событий с correlationId.
 *
 * <p>Все методы добавляют {@code correlationId} в MDC перед логированием и убирают после.
 * Это обеспечивает единый идентификатор трассировки для всех шагов обработки события.
 *
 * <p>Пример вывода:
 * <pre>
 * 2026-04-24 10:00:00.123 [main] [a1b2c3d4-...] INFO  EventLogger - [KAFKA_PUBLISH_SUCCESS] event_id=abc... type=VIEW_STARTED topic=movie-events
 * </pre>
 */
public class EventLogger {

    private static final Logger log = LoggerFactory.getLogger(EventLogger.class);
    private static final String CORRELATION_ID_KEY = CorrelationIdFilter.MDC_KEY;

    private EventLogger() {}

    /**
     * Логирует успешную публикацию события в Kafka.
     *
     * @param correlationId  ID корреляции запроса
     * @param eventId        UUID события
     * @param eventType      тип события
     * @param userId         ID пользователя
     * @param topic          Kafka-топик
     * @param partition      партиция
     * @param offset         смещение
     */
    public static void logPublishSuccess(
            String correlationId,
            String eventId,
            String eventType,
            String userId,
            String topic,
            int partition,
            long offset
    ) {
        withCorrelationId(correlationId, () ->
                log.info("[KAFKA_PUBLISH_SUCCESS] event_id={} type={} user_id={} topic={} partition={} offset={}",
                        eventId, eventType, userId, topic, partition, offset)
        );
    }

    /**
     * Логирует ошибку публикации события.
     */
    public static void logPublishFailure(
            String correlationId,
            String eventId,
            String eventType,
            String errorCode,
            String errorMessage
    ) {
        withCorrelationId(correlationId, () ->
                log.error("[KAFKA_PUBLISH_FAILURE] event_id={} type={} error_code={} error={}",
                        eventId, eventType, errorCode, errorMessage)
        );
    }

    /**
     * Логирует начало обработки HTTP-запроса.
     */
    public static void logEventReceived(String correlationId, String eventId, String eventType) {
        withCorrelationId(correlationId, () ->
                log.info("[EVENT_RECEIVED] event_id={} type={}", eventId, eventType)
        );
    }

    /**
     * Логирует ошибку валидации входного события.
     */
    public static void logValidationError(String correlationId, String field, String reason) {
        withCorrelationId(correlationId, () ->
                log.warn("[VALIDATION_ERROR] field={} reason={}", field, reason)
        );
    }

    /**
     * Логирует генерацию синтетического события (генератор).
     */
    public static void logGeneratedEvent(String eventId, String eventType, String userId, String sessionId) {
        log.debug("[GENERATOR] event_id={} type={} user_id={} session_id={}",
                eventId, eventType, userId, sessionId);
    }

    /**
     * Устанавливает correlationId в MDC, выполняет действие, убирает из MDC.
     */
    private static void withCorrelationId(String correlationId, Runnable action) {
        String existing = MDC.get(CORRELATION_ID_KEY);
        MDC.put(CORRELATION_ID_KEY, correlationId != null ? correlationId : UUID.randomUUID().toString());
        try {
            action.run();
        } finally {
            if (existing != null) {
                MDC.put(CORRELATION_ID_KEY, existing);
            } else {
                MDC.remove(CORRELATION_ID_KEY);
            }
        }
    }
}
