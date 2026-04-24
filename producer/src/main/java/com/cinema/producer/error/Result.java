package com.cinema.producer.error;

import java.util.function.Function;

/**
 * Монадический тип для явного представления результата операции или ошибки.
 *
 * <p>Заменяет использование исключений для бизнес-логики (согласно rules.md).
 * Инспирирован Either из функциональных языков и Rust Result.
 *
 * <p>Использование:
 * <pre>{@code
 * Result<String, AppError> result = kafkaService.publish(event);
 * if (result.isFailure()) {
 *     log.error("...", result.getError());
 *     return ResponseEntity.badRequest()...;
 * }
 * return ResponseEntity.ok(result.getValue());
 * }</pre>
 *
 * @param <T> тип успешного значения
 * @param <E> тип ошибки (обычно {@link AppError})
 */
public final class Result<T, E> {

    private final T value;
    private final E error;
    private final boolean success;

    private Result(T value, E error, boolean success) {
        this.value = value;
        this.error = error;
        this.success = success;
    }

    /**
     * Создаёт успешный результат.
     */
    public static <T, E> Result<T, E> success(T value) {
        return new Result<>(value, null, true);
    }

    /**
     * Создаёт результат с ошибкой.
     */
    public static <T, E> Result<T, E> failure(E error) {
        return new Result<>(null, error, false);
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isFailure() {
        return !success;
    }

    /**
     * Возвращает значение. Вызывать только после проверки {@link #isSuccess()}.
     *
     * @throws IllegalStateException если результат является ошибкой
     */
    public T getValue() {
        if (!success) {
            throw new IllegalStateException("Result содержит ошибку, не значение. Проверьте isSuccess() перед вызовом getValue().");
        }
        return value;
    }

    /**
     * Возвращает ошибку. Вызывать только после проверки {@link #isFailure()}.
     *
     * @throws IllegalStateException если результат является успешным
     */
    public E getError() {
        if (success) {
            throw new IllegalStateException("Result содержит значение, не ошибку. Проверьте isFailure() перед вызовом getError().");
        }
        return error;
    }

    /**
     * Трансформирует значение при успехе, пробрасывает ошибку без изменений.
     */
    public <U> Result<U, E> map(Function<T, U> mapper) {
        if (isFailure()) {
            return Result.failure(error);
        }
        return Result.success(mapper.apply(value));
    }

    /**
     * Трансформирует ошибку при неудаче, пробрасывает значение без изменений.
     */
    public <F> Result<T, F> mapError(Function<E, F> mapper) {
        if (isSuccess()) {
            return Result.success(value);
        }
        return Result.failure(mapper.apply(error));
    }
}
