package com.cinema.aggregation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Aggregation Service — вычисляет бизнес-метрики из ClickHouse,
 * записывает агрегаты в PostgreSQL и экспортирует в S3.
 *
 * <p>Работает независимо от Producer Service — читает напрямую из ClickHouse.
 */
@SpringBootApplication
@EnableScheduling
public class AggregationApplication {

    public static void main(String[] args) {
        SpringApplication.run(AggregationApplication.class, args);
    }
}
