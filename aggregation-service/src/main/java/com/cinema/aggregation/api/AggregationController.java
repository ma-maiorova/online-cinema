package com.cinema.aggregation.api;

import com.cinema.aggregation.domain.AggregationService;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.s3.S3ExportService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;

/**
 * HTTP API для ручного управления агрегацией и экспортом.
 *
 * <p>Эндпоинты:
 * <ul>
 *   <li>POST /api/v1/aggregation/run?date=YYYY-MM-DD — ручной пересчёт за дату</li>
 *   <li>POST /api/v1/aggregation/export?date=YYYY-MM-DD — ручной экспорт в S3</li>
 *   <li>GET  /api/v1/aggregation/health — проверка готовности</li>
 * </ul>
 */
@Tag(name = "Aggregation", description = "API для управления агрегацией бизнес-метрик и экспортом в S3")
@Slf4j
@RestController
@RequestMapping("/api/v1/aggregation")
@RequiredArgsConstructor
public class AggregationController {

    private final AggregationService aggregationService;
    private final S3ExportService s3ExportService;

    /**
     * Ручной запуск агрегации за конкретную дату.
     * Полезно для пересчёта исторических данных без ожидания расписания.
     *
     * @param date дата в формате YYYY-MM-DD (по умолчанию — вчера)
     */
    @Operation(
            summary = "Запуск агрегации бизнес-метрик",
            description = "Выполняет расчёт всех бизнес-метрик за указанную дату: " +
                    "DAU (Daily Active Users), среднее время просмотра, конверсия, топ фильмов, удержание (D1, D7). " +
                    "Результаты сохраняются в PostgreSQL. " +
                    "Если дата не указана, используется вчерашний день."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Агрегация успешно выполнена",
                    content = @Content(schema = @Schema(implementation = AggregationRunResponse.class))
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Ошибка при выполнении агрегации (ClickHouse или PostgreSQL недоступны)",
                    content = @Content(schema = @Schema(implementation = AppError.class))
            )
    })
    @PostMapping("/run")
    public ResponseEntity<?> runAggregation(
            @Parameter(
                    description = "Дата для расчёта метрик в формате YYYY-MM-DD. По умолчанию: вчера",
                    example = "2024-01-15"
            )
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @Parameter(hidden = true)
            HttpServletRequest request
    ) {
        LocalDate targetDate = date != null ? date : LocalDate.now().minusDays(1);
        log.info("[API_AGGREGATION_RUN] target_date={} requested_by={}", targetDate, request.getRemoteAddr());

        Result<Integer, AppError> result = aggregationService.aggregate(targetDate);

        if (result.isFailure()) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result.getError());
        }

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "target_date", targetDate.toString(),
                "records_written", result.getValue()
        ));
    }

    /**
     * Ручной запуск экспорта агрегатов в S3 за конкретную дату.
     * Повторный экспорт перезаписывает файл.
     *
     * @param date дата в формате YYYY-MM-DD (по умолчанию — вчера)
     */
    @Operation(
            summary = "Экспорт метрик в S3",
            description = "Экспортирует агрегированные метрики за указанную дату в S3 в формате CSV. " +
                    "Путь в S3: s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv. " +
                    "Повторный экспорт перезаписывает существующий файл. " +
                    "Если дата не указана, используется вчерашний день."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Экспорт успешно выполнен",
                    content = @Content(schema = @Schema(implementation = ExportResponse.class))
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "S3 недоступен",
                    content = @Content(schema = @Schema(implementation = AppError.class))
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Ошибка при экспорте (нет данных за указанную дату)",
                    content = @Content(schema = @Schema(implementation = AppError.class))
            )
    })
    @PostMapping("/export")
    public ResponseEntity<?> runExport(
            @Parameter(
                    description = "Дата для экспорта в формате YYYY-MM-DD. По умолчанию: вчера",
                    example = "2024-01-15"
            )
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        LocalDate exportDate = date != null ? date : LocalDate.now().minusDays(1);
        log.info("[API_EXPORT_RUN] export_date={}", exportDate);

        Result<String, AppError> result = s3ExportService.export(exportDate);

        if (result.isFailure()) {
            HttpStatus status = result.getError().code() == ErrorCode.S3_CONNECTION_FAILED
                    ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.INTERNAL_SERVER_ERROR;
            return ResponseEntity.status(status).body(result.getError());
        }

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "export_date", exportDate.toString(),
                "s3_path", result.getValue()
        ));
    }

    /**
     * Health-check.
     */
    @Operation(
            summary = "Проверка здоровья сервиса",
            description = "Возвращает статус готовности Aggregation Service. " +
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

    /**
     * DTO для ответа агрегации.
     */
    @Schema(description = "Ответ на запрос агрегации")
    private record AggregationRunResponse(
            @Schema(description = "Статус выполнения", example = "SUCCESS")
            String status,
            @Schema(description = "Целевая дата агрегации", example = "2024-01-15")
            String target_date,
            @Schema(description = "Количество записанных метрик", example = "5")
            Integer records_written
    ) {}

    /**
     * DTO для ответа экспорта.
     */
    @Schema(description = "Ответ на запрос экспорта")
    private record ExportResponse(
            @Schema(description = "Статус выполнения", example = "SUCCESS")
            String status,
            @Schema(description = "Дата экспорта", example = "2024-01-15")
            String export_date,
            @Schema(description = "Путь к файлу в S3", example = "s3://movie-analytics/daily/2024-01-15/aggregates.csv")
            String s3_path
    ) {}
}
