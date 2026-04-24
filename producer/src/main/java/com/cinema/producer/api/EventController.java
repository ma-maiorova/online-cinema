package com.cinema.producer.api;

import com.cinema.producer.domain.MovieEventRequest;
import com.cinema.producer.domain.PublishResult;
import com.cinema.producer.error.AppError;
import com.cinema.producer.error.Result;
import com.cinema.producer.kafka.KafkaProducerService;
import com.cinema.producer.logging.CorrelationIdFilter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * HTTP API для приёма событий от внешних сервисов.
 *
 * <p>POST /api/v1/events — принимает событие в JSON, валидирует, публикует в Kafka.
 * <ul>
 *   <li>200 OK + {@link PublishResult} при успехе</li>
 *   <li>400 Bad Request + {@link AppError} при ошибке валидации</li>
 *   <li>503 Service Unavailable + {@link AppError} при недоступности Kafka</li>
 * </ul>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Tag(name = "Events", description = "API для приёма и публикации событий в Kafka")
public class EventController {

    private final KafkaProducerService kafkaProducerService;

    /**
     * Принимает событие, валидирует и публикует в Kafka.
     *
     * @param request       тело запроса (JSON)
     * @param httpRequest   для извлечения correlationId из заголовка
     * @return 200 с event_id и метаданными Kafka или 400/503 с описанием ошибки
     */
    @Operation(
            summary = "Публикация события в Kafka",
            description = "Принимает событие просмотра фильма, валидирует его и публикует в Kafka topic. " +
                    "Событие сериализуется в Avro формат через Schema Registry. " +
                    "Поддерживаемые типы событий: VIEW_STARTED, VIEW_FINISHED, VIEW_PAUSED, VIEW_RESUMED, LIKED, SEARCHED. " +
                    "Поддерживаемые типы устройств: MOBILE, DESKTOP, TV, TABLET."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Событие успешно опубликовано в Kafka",
                    content = @Content(schema = @Schema(implementation = PublishResult.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Ошибка валидации запроса (невалидный event_type, device_type или отсутствуют обязательные поля)",
                    content = @Content(schema = @Schema(implementation = AppError.class))
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Kafka или Schema Registry недоступны",
                    content = @Content(schema = @Schema(implementation = AppError.class))
            )
    })
    @PostMapping
    public ResponseEntity<?> publishEvent(
            @Parameter(
                    description = "Событие просмотра фильма",
                    required = true,
                    schema = @Schema(implementation = MovieEventRequest.class)
            )
            @Valid @RequestBody MovieEventRequest request,
            @Parameter(
                    description = "HTTP запрос для извлечения correlationId из заголовка X-Correlation-ID",
                    hidden = true
            )
            HttpServletRequest httpRequest
    ) {
        String correlationId = httpRequest.getHeader(CorrelationIdFilter.CORRELATION_ID_HEADER);

        Result<PublishResult, AppError> result = kafkaProducerService.publish(request, correlationId);

        if (result.isFailure()) {
            AppError error = result.getError();
            HttpStatus status = resolveHttpStatus(error);
            return ResponseEntity.status(status).body(error);
        }

        return ResponseEntity.ok(result.getValue());
    }

    /**
     * Health-check endpoint для проверки готовности сервиса.
     */
    @Operation(
            summary = "Проверка здоровья сервиса",
            description = "Возвращает статус готовности Producer Service. " +
                    "Используется для health-check в Kubernetes и Docker Compose."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Сервис работает нормально",
            content = @Content(schema = @Schema(type = "string", example = "UP"))
    )
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("UP");
    }

    private HttpStatus resolveHttpStatus(AppError error) {
        return switch (error.code()) {
            case VALIDATION_FAILED, INVALID_EVENT_TYPE, INVALID_DEVICE_TYPE, MISSING_REQUIRED_FIELD ->
                    HttpStatus.BAD_REQUEST;
            case KAFKA_PUBLISH_FAILED, SCHEMA_REGISTRY_UNAVAILABLE ->
                    HttpStatus.SERVICE_UNAVAILABLE;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
