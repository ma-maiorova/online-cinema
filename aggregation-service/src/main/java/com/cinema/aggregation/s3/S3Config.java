package com.cinema.aggregation.s3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;

/**
 * Конфигурация AWS S3 Client для MinIO (или любого S3-совместимого хранилища).
 *
 * <p>Использует UrlConnectionHttpClient для совместимости с MinIO в Docker.
 * Параметры берутся из environment variables (S3_ENDPOINT, S3_ACCESS_KEY и т.д.).
 */
@Slf4j
@Configuration
public class S3Config {

    @Value("${cinema.s3.endpoint}")
    private String endpoint;

    @Value("${cinema.s3.access-key}")
    private String accessKey;

    @Value("${cinema.s3.secret-key}")
    private String secretKey;

    @Value("${cinema.s3.region:us-east-1}")
    private String region;

    @Bean
    public S3Client s3Client() {
        log.info("[S3_CONFIG] Подключение к S3-совместимому хранилищу: {}", endpoint);

        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ))
                .region(Region.of(region))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)  // Обязательно для MinIO
                        .build())
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .build();
    }
}
