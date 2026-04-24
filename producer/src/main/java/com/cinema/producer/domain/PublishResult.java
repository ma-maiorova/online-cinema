package com.cinema.producer.domain;

/**
 * Результат успешной публикации события в Kafka.
 *
 * @param eventId   UUID опубликованного события
 * @param topic     Kafka-топик
 * @param partition партиция
 * @param offset    смещение записи
 */
public record PublishResult(
        String eventId,
        String topic,
        int partition,
        long offset
) {}
