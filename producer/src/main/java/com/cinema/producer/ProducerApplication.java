package com.cinema.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Movie Producer Service — публикует события пользователей в Kafka topic movie-events.
 *
 * <p>Два режима работы:
 * <ul>
 *   <li>HTTP API — принимает события от внешних сервисов (POST /api/v1/events)</li>
 *   <li>Генератор — самостоятельно генерирует синтетический поток событий</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
