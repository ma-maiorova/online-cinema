package com.cinema.aggregation.error;

import java.util.Collections;
import java.util.Map;

/**
 * Структурированная ошибка Aggregation Service.
 *
 * <p>Используется вместо исключений для бизнес-сценариев (согласно rules.md).
 */
public record AppError(
        ErrorCode code,
        String message,
        Map<String, Object> details
) {
    public static AppError of(ErrorCode code) {
        return new AppError(code, code.getDefaultMessage(), Collections.emptyMap());
    }

    public static AppError of(ErrorCode code, String message) {
        return new AppError(code, message, Collections.emptyMap());
    }

    public static AppError of(ErrorCode code, String message, Map<String, Object> details) {
        return new AppError(code, message, Collections.unmodifiableMap(details));
    }

    public static AppError of(ErrorCode code, Throwable cause) {
        return new AppError(code, code.getDefaultMessage() + ": " + cause.getMessage(), Collections.emptyMap());
    }
}
