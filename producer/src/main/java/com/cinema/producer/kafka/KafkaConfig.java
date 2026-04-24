package com.cinema.producer.kafka;

import com.cinema.avro.MovieEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация Kafka Producer.
 *
 * <p>Ключевые параметры:
 * <ul>
 *   <li>acks=all — подтверждение от всех ISR реплик (надёжность)</li>
 *   <li>retries=3 + retry.backoff.ms=1000 — exponential backoff при ошибках</li>
 *   <li>KafkaAvroSerializer — сериализация с Schema Registry</li>
 * </ul>
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${cinema.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, MovieEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        // Надёжность: acks=all гарантирует запись во все ISR реплики
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry с exponential backoff
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);

        // Batching для производительности
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // Schema Registry
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", true);
        props.put("use.latest.version", false);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, MovieEvent> kafkaTemplate(ProducerFactory<String, MovieEvent> factory) {
        return new KafkaTemplate<>(factory);
    }
}
