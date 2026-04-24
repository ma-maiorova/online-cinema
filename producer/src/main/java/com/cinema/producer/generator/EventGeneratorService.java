package com.cinema.producer.generator;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.avro.MovieEvent;
import com.cinema.producer.error.Result;
import com.cinema.producer.error.AppError;
import com.cinema.producer.kafka.KafkaProducerService;
import com.cinema.producer.logging.EventLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Генератор синтетических событий для наполнения pipeline данными.
 *
 * <p>Имитирует реалистичное поведение пользователей:
 * <ul>
 *   <li>VIEW_STARTED всегда предшествует VIEW_FINISHED в рамках сессии</li>
 *   <li>progress_seconds монотонно растёт между событиями</li>
 *   <li>Случайные паузы VIEW_PAUSED → VIEW_RESUMED</li>
 *   <li>LIKED и SEARCHED генерируются независимо от сессии просмотра</li>
 * </ul>
 *
 * <p>Включается через {@code GENERATOR_ENABLED=true} (по умолчанию — true).
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "cinema.generator.enabled", havingValue = "true", matchIfMissing = true)
public class EventGeneratorService {

    private static final DeviceType[] DEVICE_TYPES = DeviceType.values();
    private static final int PROGRESS_STEP_MIN = 10;
    private static final int PROGRESS_STEP_MAX = 60;
    private static final int MOVIE_DURATION_MIN = 3600;
    private static final int MOVIE_DURATION_MAX = 7200;

    private final KafkaProducerService kafkaProducerService;
    private final Random random = new Random();

    // Активные сессии: userId → SessionState
    private final ConcurrentMap<String, SessionState> activeSessions = new ConcurrentHashMap<>();

    private final List<String> userIds;
    private final List<String> movieIds;

    @Value("${cinema.generator.events-per-second:5}")
    private int eventsPerSecond;

    public EventGeneratorService(
            KafkaProducerService kafkaProducerService,
            @Value("${cinema.generator.user-count:20}") int userCount,
            @Value("${cinema.generator.movie-count:10}") int movieCount
    ) {
        this.kafkaProducerService = kafkaProducerService;
        this.userIds = generateIds("user-", userCount);
        this.movieIds = generateIds("movie-", movieCount);
        log.info("[GENERATOR_INIT] users={} movies={}", userCount, movieCount);
    }

    /**
     * Запускается каждую секунду, генерирует пакет событий.
     */
    @Scheduled(fixedRateString = "1000")
    public void generateBatch() {
        for (int i = 0; i < eventsPerSecond; i++) {
            generateOneEvent();
        }
    }

    private void generateOneEvent() {
        String userId = randomFrom(userIds);

        // С вероятностью 30% — поиск или лайк (независимые события)
        if (random.nextInt(10) < 3) {
            publishIndependentEvent(userId);
            return;
        }

        SessionState session = activeSessions.get(userId);

        if (session == null) {
            startNewSession(userId);
        } else if (session.paused()) {
            resumeOrFinish(userId, session);
        } else if (session.progressSeconds() >= session.movieDuration()) {
            finishSession(userId, session);
        } else {
            continueOrPause(userId, session);
        }
    }

    private void startNewSession(String userId) {
        String sessionId = UUID.randomUUID().toString();
        String movieId = randomFrom(movieIds);
        DeviceType device = DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)];
        int duration = MOVIE_DURATION_MIN + random.nextInt(MOVIE_DURATION_MAX - MOVIE_DURATION_MIN);

        SessionState state = new SessionState(userId, movieId, sessionId, device, 0, false, false, duration);
        activeSessions.put(userId, state.withStarted());

        publishEvent(userId, movieId, EventType.VIEW_STARTED, device, sessionId, 0);
    }

    private void continueOrPause(String userId, SessionState session) {
        int step = PROGRESS_STEP_MIN + random.nextInt(PROGRESS_STEP_MAX - PROGRESS_STEP_MIN);
        int newProgress = Math.min(session.progressSeconds() + step, session.movieDuration());

        // С вероятностью 15% — поставить на паузу
        if (random.nextInt(100) < 15) {
            activeSessions.put(userId, session.withProgress(newProgress).withPaused(true));
            publishEvent(userId, session.movieId(), EventType.VIEW_PAUSED,
                    session.deviceType(), session.sessionId(), newProgress);
        } else {
            activeSessions.put(userId, session.withProgress(newProgress));
            // Не публикуем событие за каждый шаг — только VIEW_PAUSED, VIEW_RESUMED, VIEW_FINISHED
        }
    }

    private void resumeOrFinish(String userId, SessionState session) {
        int step = PROGRESS_STEP_MIN + random.nextInt(PROGRESS_STEP_MAX - PROGRESS_STEP_MIN);
        int newProgress = Math.min(session.progressSeconds() + step, session.movieDuration());

        activeSessions.put(userId, session.withProgress(newProgress).withPaused(false));
        publishEvent(userId, session.movieId(), EventType.VIEW_RESUMED,
                session.deviceType(), session.sessionId(), newProgress);
    }

    private void finishSession(String userId, SessionState session) {
        publishEvent(userId, session.movieId(), EventType.VIEW_FINISHED,
                session.deviceType(), session.sessionId(), session.movieDuration());
        activeSessions.remove(userId);
    }

    private void publishIndependentEvent(String userId) {
        EventType type = random.nextBoolean() ? EventType.LIKED : EventType.SEARCHED;
        String movieId = randomFrom(movieIds);
        DeviceType device = DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)];
        String sessionId = UUID.randomUUID().toString();
        publishEvent(userId, movieId, type, device, sessionId, 0);
    }

    private void publishEvent(
            String userId,
            String movieId,
            EventType eventType,
            DeviceType deviceType,
            String sessionId,
            int progressSeconds
    ) {
        String eventId = UUID.randomUUID().toString();

        MovieEvent event = MovieEvent.newBuilder()
                .setEventId(eventId)
                .setUserId(userId)
                .setMovieId(movieId)
                .setEventType(eventType)
                .setTimestamp(Instant.now())
                .setDeviceType(deviceType)
                .setSessionId(sessionId)
                .setProgressSeconds(progressSeconds)
                .build();

        // correlationId для генератора = eventId (нет HTTP-контекста)
        Result<?, AppError> result = kafkaProducerService.publishAvroEvent(event, eventId);

        if (result.isFailure()) {
            log.warn("[GENERATOR_PUBLISH_FAIL] event_id={} type={} error={}",
                    eventId, eventType, result.getError().message());
        } else {
            EventLogger.logGeneratedEvent(eventId, eventType.toString(), userId, sessionId);
        }
    }

    private List<String> generateIds(String prefix, int count) {
        List<String> ids = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            ids.add(prefix + i);
        }
        return ids;
    }

    private <T> T randomFrom(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }
}
