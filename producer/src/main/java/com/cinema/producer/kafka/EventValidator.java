package com.cinema.producer.kafka;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import com.cinema.producer.error.Result;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Валидатор полей события перед сериализацией в Avro.
 *
 * <p>Возвращает {@link Result} — не бросает исключений (согласно rules.md).
 */
@Component
public class EventValidator {

    private static final String VALID_EVENT_TYPES = Arrays.stream(EventType.values())
            .map(Enum::name)
            .collect(Collectors.joining(", "));

    private static final String VALID_DEVICE_TYPES = Arrays.stream(DeviceType.values())
            .map(Enum::name)
            .collect(Collectors.joining(", "));

    /**
     * Парсит строку в {@link EventType}, возвращает ошибку если значение неизвестно.
     */
    public Result<EventType, AppError> parseEventType(String value) {
        try {
            return Result.success(EventType.valueOf(value));
        } catch (IllegalArgumentException e) {
            return Result.failure(AppError.of(
                    ErrorCode.INVALID_EVENT_TYPE,
                    "Неизвестный event_type: '" + value + "'. Допустимые значения: " + VALID_EVENT_TYPES,
                    java.util.Map.of("received", value, "allowed", VALID_EVENT_TYPES)
            ));
        }
    }

    /**
     * Парсит строку в {@link DeviceType}, возвращает ошибку если значение неизвестно.
     */
    public Result<DeviceType, AppError> parseDeviceType(String value) {
        try {
            return Result.success(DeviceType.valueOf(value));
        } catch (IllegalArgumentException e) {
            return Result.failure(AppError.of(
                    ErrorCode.INVALID_DEVICE_TYPE,
                    "Неизвестный device_type: '" + value + "'. Допустимые значения: " + VALID_DEVICE_TYPES,
                    java.util.Map.of("received", value, "allowed", VALID_DEVICE_TYPES)
            ));
        }
    }
}
