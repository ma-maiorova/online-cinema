package com.cinema.producer.config;

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
 * Конфигурация OpenAPI (Swagger) для Producer Service.
 *
 * <p>Доступ к Swagger UI: http://localhost:8080/swagger-ui.html
 * <p>OpenAPI JSON: http://localhost:8080/v3/api-docs
 */
@Configuration
public class OpenApiConfig {

    @Value("${server.port:8080}")
    private int serverPort;

    @Bean
    public OpenAPI producerOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Online Cinema - Producer Service API")
                        .description("""
                                Kafka Producer Service для онлайн-кинотеатра.
                                
                                **Функционал:**
                                - Приём событий от внешних сервисов через HTTP API
                                - Публикация событий в Kafka topic `movie-events`
                                - Генерация синтетических событий для тестирования
                                
                                **Схема события (Avro):**
                                - event_id: UUID
                                - user_id: string (ключ партиционирования)
                                - movie_id: string
                                - event_type: VIEW_STARTED | VIEW_FINISHED | VIEW_PAUSED | VIEW_RESUMED | LIKED | SEARCHED
                                - timestamp: datetime (UTC)
                                - device_type: MOBILE | DESKTOP | TV | TABLET
                                - session_id: string
                                - progress_seconds: integer
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
                        new Server().url("http://producer:8080").description("Docker Internal")
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

                // 400 Bad Request - ошибка валидации
                apiResponses.addApiResponse("400", new ApiResponse()
                        .description("Bad Request - ошибка валидации входных данных")
                        .content(new Content()
                                .addMediaType("application/json", new MediaType()
                                        .schema(new Schema<>().$ref("#/components/schemas/AppError")))));

                // 503 Service Unavailable - Kafka недоступен
                apiResponses.addApiResponse("503", new ApiResponse()
                        .description("Service Unavailable - Kafka или Schema Registry недоступны")
                        .content(new Content()
                                .addMediaType("application/json", new MediaType()
                                        .schema(new Schema<>().$ref("#/components/schemas/AppError")))));
            }));
        };
    }
}
