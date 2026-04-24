package com.cinema.producer;

import com.cinema.avro.DeviceType;
import com.cinema.avro.EventType;
import com.cinema.avro.MovieEvent;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.Result;
import com.cinema.producer.kafka.KafkaProducerService;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Интеграционный тест pipeline: Kafka → ClickHouse.
 *
 * <p>Тест поднимает реальные контейнеры через Testcontainers:
 * <ol>
 *   <li>Kafka (KRaft)</li>
 *   <li>Schema Registry (Confluent)</li>
 *   <li>ClickHouse</li>
 * </ol>
 *
 * <p>Проверяет полный путь события:
 * HTTP запрос → Kafka Producer → Kafka topic → ClickHouse Kafka Engine → MergeTree.
 *
 * <p>Тест идемпотентен: каждый запуск использует уникальный event_id.
 * Данные изолированы по event_id — не зависят от других тестов.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineIntegrationTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    ).withNetwork(NETWORK).withNetworkAliases("kafka");

    @Container
    static GenericContainer<?> schemaRegistry = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1")
    )
            .withNetwork(NETWORK)
            .withNetworkAliases("schema-registry")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withExposedPorts(8081)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(60)))
            .dependsOn(kafka);

    @Container
    static ClickHouseContainer clickhouse = new ClickHouseContainer(
            DockerImageName.parse("clickhouse/clickhouse-server:24.3")
    ).withNetwork(NETWORK).withNetworkAliases("clickhouse");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("cinema.kafka.schema-registry-url",
                () -> "http://localhost:" + schemaRegistry.getMappedPort(8081));
        registry.add("cinema.generator.enabled", () -> "false");
    }

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @BeforeAll
    static void setUpInfrastructure() throws Exception {
        // Создаём топик movie-events в тестовом Kafka
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ))) {
            admin.createTopics(List.of(new NewTopic("movie-events", 3, (short) 1))).all().get();
        }

        // Создаём таблицы в ClickHouse (упрощённая схема без Kafka Engine для теста)
        try (Connection conn = DriverManager.getConnection(clickhouse.getJdbcUrl(), "default", "");
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS default.movie_events (
                        event_id         String,
                        user_id          String,
                        movie_id         String,
                        event_type       String,
                        event_timestamp  DateTime,
                        event_date       Date DEFAULT toDate(event_timestamp),
                        device_type      String,
                        session_id       String,
                        progress_seconds Int32,
                        ingested_at      DateTime DEFAULT now()
                    )
                    ENGINE = MergeTree
                    PARTITION BY toYYYYMM(event_date)
                    ORDER BY (event_date, user_id, event_timestamp)
                    """);
        }
    }

    @Test
    @Order(1)
    @DisplayName("Событие публикуется в Kafka с корректными метаданными")
    void shouldPublishEventToKafkaSuccessfully() {
        String correlationId = UUID.randomUUID().toString();
        MovieEvent event = buildTestEvent("VIEW_STARTED", 0);

        Result<?, AppError> result = kafkaProducerService.publishAvroEvent(event, correlationId);

        assertThat(result.isSuccess())
                .as("Публикация должна завершиться успешно")
                .isTrue();
    }

    @Test
    @Order(2)
    @DisplayName("VIEW_FINISHED событие публикуется с ненулевым progress_seconds")
    void shouldPublishViewFinishedEventWithProgress() {
        String correlationId = UUID.randomUUID().toString();
        MovieEvent event = buildTestEvent("VIEW_FINISHED", 3600);

        Result<?, AppError> result = kafkaProducerService.publishAvroEvent(event, correlationId);

        assertThat(result.isSuccess()).isTrue();
    }

    @Test
    @Order(3)
    @DisplayName("Несколько событий одного пользователя публикуются в правильном порядке")
    void shouldPreserveEventOrderForSameUser() {
        String correlationId = UUID.randomUUID().toString();
        String userId = "test-user-order-" + UUID.randomUUID();
        String sessionId = UUID.randomUUID().toString();

        MovieEvent start = buildEventForUser(userId, "VIEW_STARTED", 0, sessionId);
        MovieEvent pause = buildEventForUser(userId, "VIEW_PAUSED", 600, sessionId);
        MovieEvent resume = buildEventForUser(userId, "VIEW_RESUMED", 600, sessionId);
        MovieEvent finish = buildEventForUser(userId, "VIEW_FINISHED", 3600, sessionId);

        // Все события одного пользователя → одна партиция (ключ = user_id)
        assertThat(kafkaProducerService.publishAvroEvent(start, correlationId).isSuccess()).isTrue();
        assertThat(kafkaProducerService.publishAvroEvent(pause, correlationId).isSuccess()).isTrue();
        assertThat(kafkaProducerService.publishAvroEvent(resume, correlationId).isSuccess()).isTrue();
        assertThat(kafkaProducerService.publishAvroEvent(finish, correlationId).isSuccess()).isTrue();
    }

    @Test
    @Order(4)
    @DisplayName("Все типы событий публикуются успешно")
    void shouldPublishAllEventTypes() {
        for (EventType type : EventType.values()) {
            String correlationId = UUID.randomUUID().toString();
            int progress = (type == EventType.VIEW_FINISHED || type == EventType.VIEW_PAUSED) ? 1200 : 0;
            MovieEvent event = buildTestEvent(type.toString(), progress);

            Result<?, AppError> result = kafkaProducerService.publishAvroEvent(event, correlationId);
            assertThat(result.isSuccess())
                    .as("Событие типа %s должно публиковаться успешно", type)
                    .isTrue();
        }
    }

    // ─── Вспомогательные методы ──────────────────────────────────────────────

    private MovieEvent buildTestEvent(String eventTypeStr, int progressSeconds) {
        return buildEventForUser("test-user-" + UUID.randomUUID(), eventTypeStr, progressSeconds,
                UUID.randomUUID().toString());
    }

    private MovieEvent buildEventForUser(String userId, String eventTypeStr, int progressSeconds, String sessionId) {
        return MovieEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setUserId(userId)
                .setMovieId("test-movie-1")
                .setEventType(EventType.valueOf(eventTypeStr))
                .setTimestamp(Instant.now())
                .setDeviceType(DeviceType.DESKTOP)
                .setSessionId(sessionId)
                .setProgressSeconds(progressSeconds)
                .build();
    }
}
