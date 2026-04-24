package com.cinema.producer.error;

/**
 * Коды ошибок Producer Service.
 *
 * <p>Используются в {@link AppError} для машино-читаемой идентификации ошибки.
 * Каждый код несёт семантику и HTTP-статус по умолчанию.
 */
public enum ErrorCode {

    // Валидация входных данных
    VALIDATION_FAILED("VALIDATION_FAILED", "Некорректные входные данные"),
    MISSING_REQUIRED_FIELD("MISSING_REQUIRED_FIELD", "Отсутствует обязательное поле"),
    INVALID_EVENT_TYPE("INVALID_EVENT_TYPE", "Неизвестный тип события"),
    INVALID_DEVICE_TYPE("INVALID_DEVICE_TYPE", "Неизвестный тип устройства"),

    // Kafka
    KAFKA_PUBLISH_FAILED("KAFKA_PUBLISH_FAILED", "Ошибка публикации сообщения в Kafka"),
    KAFKA_SERIALIZATION_FAILED("KAFKA_SERIALIZATION_FAILED", "Ошибка сериализации события"),
    SCHEMA_REGISTRY_UNAVAILABLE("SCHEMA_REGISTRY_UNAVAILABLE", "Schema Registry недоступен"),

    // Внутренние
    INTERNAL_ERROR("INTERNAL_ERROR", "Внутренняя ошибка сервиса");

    private final String code;
    private final String defaultMessage;

    ErrorCode(String code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    public String getCode() {
        return code;
    }

    public String getDefaultMessage() {
        return defaultMessage;
    }
}
