package com.cinema.aggregation.error;

/**
 * Коды ошибок Aggregation Service.
 */
public enum ErrorCode {
    CLICKHOUSE_QUERY_FAILED("CLICKHOUSE_QUERY_FAILED", "Ошибка запроса к ClickHouse"),
    CLICKHOUSE_CONNECTION_FAILED("CLICKHOUSE_CONNECTION_FAILED", "Нет соединения с ClickHouse"),
    POSTGRES_WRITE_FAILED("POSTGRES_WRITE_FAILED", "Ошибка записи в PostgreSQL"),
    POSTGRES_CONNECTION_FAILED("POSTGRES_CONNECTION_FAILED", "Нет соединения с PostgreSQL"),
    S3_UPLOAD_FAILED("S3_UPLOAD_FAILED", "Ошибка загрузки файла в S3"),
    S3_CONNECTION_FAILED("S3_CONNECTION_FAILED", "S3/MinIO недоступен"),
    AGGREGATION_FAILED("AGGREGATION_FAILED", "Ошибка вычисления агрегата"),
    INVALID_DATE("INVALID_DATE", "Некорректная дата"),
    INTERNAL_ERROR("INTERNAL_ERROR", "Внутренняя ошибка сервиса");

    private final String code;
    private final String defaultMessage;

    ErrorCode(String code, String defaultMessage) {
        this.code = code;
        this.defaultMessage = defaultMessage;
    }

    public String getCode() { return code; }
    public String getDefaultMessage() { return defaultMessage; }
}
