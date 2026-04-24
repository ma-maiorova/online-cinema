package com.cinema.producer.generator;

import com.cinema.avro.DeviceType;

/**
 * Состояние текущей пользовательской сессии в генераторе событий.
 *
 * <p>Генератор поддерживает реалистичную последовательность событий:
 * VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED
 * Внутри одной сессии progress_seconds только растёт.
 *
 * @param userId          ID пользователя
 * @param movieId         ID текущего фильма
 * @param sessionId       UUID сессии
 * @param deviceType      тип устройства
 * @param progressSeconds текущий прогресс просмотра в секундах
 * @param started         была ли уже отправлена VIEW_STARTED
 * @param paused          сейчас на паузе
 * @param movieDuration   продолжительность фильма в секундах (случайная)
 */
public record SessionState(
        String userId,
        String movieId,
        String sessionId,
        DeviceType deviceType,
        int progressSeconds,
        boolean started,
        boolean paused,
        int movieDuration
) {
    public SessionState withProgress(int newProgress) {
        return new SessionState(userId, movieId, sessionId, deviceType, newProgress, started, paused, movieDuration);
    }

    public SessionState withStarted() {
        return new SessionState(userId, movieId, sessionId, deviceType, progressSeconds, true, paused, movieDuration);
    }

    public SessionState withPaused(boolean isPaused) {
        return new SessionState(userId, movieId, sessionId, deviceType, progressSeconds, started, isPaused, movieDuration);
    }
}
