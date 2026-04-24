package com.cinema.producer.domain;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * DTO входящего HTTP-запроса на публикацию события.
 *
 * <p>Все поля non-null по умолчанию (согласно rules.md). Nullable — только те,
 * что опционально присутствуют в бизнес-логике.
 *
 * @param eventId         UUID события (если null — генерируется автоматически)
 * @param userId          ID пользователя
 * @param movieId         ID фильма
 * @param eventType       тип события
 * @param deviceType      тип устройства
 * @param sessionId       ID сессии
 * @param progressSeconds прогресс просмотра в секундах
 */
public record MovieEventRequest(
        @Nullable String eventId,
        @NotBlank String userId,
        @NotBlank String movieId,
        @NotNull String eventType,
        @NotNull String deviceType,
        @NotBlank String sessionId,
        int progressSeconds
) {}
