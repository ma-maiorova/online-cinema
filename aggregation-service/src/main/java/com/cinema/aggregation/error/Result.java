package com.cinema.aggregation.error;

import java.util.function.Function;

/**
 * Монадический тип Result для явного представления результата или ошибки.
 *
 * <p>Идентичен по семантике с producer/Result, дублируется намеренно
 * (сервисы не зависят друг от друга).
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

    public static <T, E> Result<T, E> success(T value) {
        return new Result<>(value, null, true);
    }

    public static <T, E> Result<T, E> failure(E error) {
        return new Result<>(null, error, false);
    }

    public boolean isSuccess() { return success; }
    public boolean isFailure() { return !success; }

    public T getValue() {
        if (!success) throw new IllegalStateException("Result содержит ошибку. Проверьте isSuccess().");
        return value;
    }

    public E getError() {
        if (success) throw new IllegalStateException("Result содержит значение. Проверьте isFailure().");
        return error;
    }

    public <U> Result<U, E> map(Function<T, U> mapper) {
        if (isFailure()) return Result.failure(error);
        return Result.success(mapper.apply(value));
    }
}
