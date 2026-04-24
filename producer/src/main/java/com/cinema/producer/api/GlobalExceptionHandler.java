package com.cinema.producer.api;

import com.cinema.producer.error.AppError;
import com.cinema.producer.error.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

/**
 * Глобальный обработчик исключений Spring MVC.
 *
 * <p>Конвертирует технические исключения валидации в единый формат {@link AppError}.
 * Бизнес-ошибки обрабатываются через {@link com.cinema.producer.error.Result} —
 * до этого обработчика не доходят.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Обрабатывает ошибки Bean Validation (@Valid аннотации).
     * Конвертирует в AppError с деталями по каждому полю.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<AppError> handleValidationException(MethodArgumentNotValidException ex) {
        Map<String, Object> details = new HashMap<>();
        for (FieldError fieldError : ex.getBindingResult().getFieldErrors()) {
            details.put(fieldError.getField(), fieldError.getDefaultMessage());
        }

        AppError error = AppError.of(
                ErrorCode.VALIDATION_FAILED,
                "Ошибка валидации входных данных",
                details
        );

        log.warn("[VALIDATION_EXCEPTION] fields={}", details);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    /**
     * Fallback-обработчик для непредвиденных исключений.
     * Не раскрывает внутренние детали клиенту.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<AppError> handleGenericException(Exception ex) {
        log.error("[UNHANDLED_EXCEPTION] {}", ex.getMessage(), ex);
        AppError error = AppError.of(ErrorCode.INTERNAL_ERROR, "Внутренняя ошибка сервиса");
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
    }
}
