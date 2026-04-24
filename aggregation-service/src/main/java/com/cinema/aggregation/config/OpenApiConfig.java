package com.cinema.aggregation.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Конфигурация OpenAPI (Swagger) для Aggregation Service.
 *
 * <p>Доступ к Swagger UI: http://localhost:8082/swagger-ui.html
 * <p>OpenAPI JSON: http://localhost:8082/v3/api-docs
 */
@Configuration
public class OpenApiConfig {

    @Value("${server.port:8082}")
    private int serverPort;

    @Bean
    public OpenAPI aggregationOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Online Cinema - Aggregation Service API")
                        .description("""
                                Aggregation Service для онлайн-кинотеатра.
                                
                                **Функционал:**
                                - Чтение raw-событий из ClickHouse
                                - Вычисление бизнес-метрик (DAU, retention, конверсия, топ фильмов)
                                - Запись агрегатов в PostgreSQL
                                - Экспорт метрик в S3/MinIO
                                
                                **Бизнес-метрики:**
                                - DAU: количество уникальных пользователей за день
                                - Среднее время просмотра: средний progress_seconds для VIEW_FINISHED
                                - Топ фильмов: рейтинг по количеству просмотров
                                - Конверсия: доля VIEW_FINISHED от VIEW_STARTED
                                - Retention D1/D7: доля пользователей, вернувшихся через 1 и 7 дней
                                
                                **Расписание:**
                                - Автоматический запуск по cron (настраивается через AGGREGATION_SCHEDULE_CRON)
                                - Ручной запуск через API endpoint
                                """)
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Cinema Team")
                                .email("dev@cinema.example.com"))
                        .license(new License()
                                .name("MIT License")
                                .url("https://opensource.org/licenses/MIT")))
                .servers(List.of(
                        new Server().url("http://localhost:" + serverPort).description("Local Development"),
                        new Server().url("http://aggregation-service:8082").description("Docker Internal")
                ));
    }

    /**
     * Кастомайзер для добавления общих ответов на ошибки.
     */
    @Bean
    public OpenApiCustomizer globalErrorResponseCustomizer() {
        return openApi -> {
            openApi.getPaths().values().forEach(pathItem -> pathItem.readOperations().forEach(operation -> {
                ApiResponses apiResponses = operation.getResponses();

                // 400 Bad Request - неверный формат даты
                apiResponses.addApiResponse("400", new ApiResponse()
                        .description("Bad Request - неверный формат параметров запроса")
                        .content(new Content()
                                .addMediaType("application/json", new MediaType()
                                        .schema(new Schema<>().$ref("#/components/schemas/AppError")))));

                // 500 Internal Server Error - ошибка агрегации
                apiResponses.addApiResponse("500", new ApiResponse()
                        .description("Internal Server Error - ошибка при вычислении метрик")
                        .content(new Content()
                                .addMediaType("application/json", new MediaType()
                                        .schema(new Schema<>().$ref("#/components/schemas/AppError")))));

                // 503 Service Unavailable - ClickHouse, PostgreSQL или S3 недоступны
                apiResponses.addApiResponse("503", new ApiResponse()
                        .description("Service Unavailable - внешние сервисы недоступны")
                        .content(new Content()
                                .addMediaType("application/json", new MediaType()
                                        .schema(new Schema<>().$ref("#/components/schemas/AppError")))));
            }));
        };
    }
}
