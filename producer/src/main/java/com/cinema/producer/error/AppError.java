package com.cinema.producer.error;

import java.util.Collections;
import java.util.Map;

/**
 * Структурированная ошибка приложения.
 *
 * <p>Используется вместо исключений для бизнес-сценариев (согласно rules.md).
 * Содержит машино-читаемый код, человеко-читаемое сообщение и опциональные детали.
 *
 * <p>Пример сериализации в JSON:
 * <pre>{@code
 * {
 *   "code": "VALIDATION_FAILED",
 *   "message": "Некорректные входные данные",
 *   "details": { "field": "event_type", "value": "UNKNOWN" }
 * }
 * }</pre>
 */
public record AppError(
        ErrorCode code,
        String message,
        Map<String, Object> details
) {

    /**
     * Создаёт ошибку с сообщением по умолчанию из {@link ErrorCode}.
     */
    public static AppError of(ErrorCode code) {
        return new AppError(code, code.getDefaultMessage(), Collections.emptyMap());
    }

    /**
     * Создаёт ошибку с произвольным сообщением.
     */
    public static AppError of(ErrorCode code, String message) {
        return new AppError(code, message, Collections.emptyMap());
    }

    /**
     * Создаёт ошибку с деталями (например, имя поля, значение).
     */
    public static AppError of(ErrorCode code, String message, Map<String, Object> details) {
        return new AppError(code, message, Collections.unmodifiableMap(details));
    }

    /**
     * Создаёт ошибку валидации с указанием конкретного поля.
     */
    public static AppError validationFailed(String field, Object invalidValue) {
        return new AppError(
                ErrorCode.VALIDATION_FAILED,
                "Некорректное значение поля '" + field + "'",
                Map.of("field", field, "value", String.valueOf(invalidValue))
        );
    }
}
