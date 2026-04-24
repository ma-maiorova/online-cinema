package com.cinema.aggregation.s3;

import com.cinema.aggregation.domain.DailyMetric;
import com.cinema.aggregation.error.AppError;
import com.cinema.aggregation.error.ErrorCode;
import com.cinema.aggregation.error.Result;
import com.cinema.aggregation.logging.AggregationLogger;
import com.cinema.aggregation.postgres.MetricsRepository;
import com.opencsv.CSVWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.StringWriter;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * Сервис экспорта агрегатов из PostgreSQL в S3/MinIO.
 *
 * <p>Формат: CSV. Путь: {@code s3://movie-analytics/daily/YYYY-MM-DD/aggregates.csv}.
 * Повторный экспорт за ту же дату перезаписывает файл (идемпотентность).
 *
 * <p>Обработка ошибок:
 * <ul>
 *   <li>PostgreSQL недоступен → {@link Result#failure} с кодом POSTGRES_CONNECTION_FAILED</li>
 *   <li>S3 недоступен → {@link Result#failure} с кодом S3_CONNECTION_FAILED</li>
 * </ul>
 */
@Slf4j
@Service
public class S3ExportService {

    private static final String S3_KEY_PATTERN = "daily/%s/aggregates.csv";
    private static final String[] CSV_HEADER = {
            "metric_date", "metric_name", "dimension_key", "metric_value", "computed_at"
    };

    private final S3Client s3Client;
    private final MetricsRepository metricsRepository;

    @Value("${cinema.s3.bucket}")
    private String bucket;

    public S3ExportService(S3Client s3Client, MetricsRepository metricsRepository) {
        this.s3Client = s3Client;
        this.metricsRepository = metricsRepository;
    }

    /**
     * Экспортирует метрики за указанную дату в S3.
     *
     * @param exportDate дата экспорта
     * @return Result с S3-путём файла или ошибкой
     */
    public Result<String, AppError> export(LocalDate exportDate) {
        String correlationId = UUID.randomUUID().toString();
        String s3Key = String.format(S3_KEY_PATTERN, exportDate);

        // 1. Читаем метрики из PostgreSQL
        Result<List<DailyMetric>, AppError> metricsResult = metricsRepository.findMetricsByDate(exportDate);
        if (metricsResult.isFailure()) {
            AggregationLogger.logS3ExportFailure(correlationId, exportDate, metricsResult.getError().message());
            return Result.failure(metricsResult.getError());
        }

        List<DailyMetric> metrics = metricsResult.getValue();
        if (metrics.isEmpty()) {
            log.warn("[S3_EXPORT_SKIP] date={} reason=no_metrics_found", exportDate);
            return Result.success("s3://" + bucket + "/" + s3Key + " (skipped: no data)");
        }

        // 2. Формируем CSV
        String csvContent = buildCsv(metrics);

        // 3. Загружаем в S3
        Result<String, AppError> uploadResult = uploadToS3(s3Key, csvContent, correlationId, exportDate);
        if (uploadResult.isFailure()) {
            return uploadResult;
        }

        // 4. Записываем в PostgreSQL историю экспорта
        metricsRepository.recordS3Export(exportDate, uploadResult.getValue());

        AggregationLogger.logS3ExportSuccess(correlationId, exportDate, uploadResult.getValue());
        return uploadResult;
    }

    private Result<String, AppError> uploadToS3(String s3Key, String csvContent,
                                                  String correlationId, LocalDate exportDate) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(s3Key)
                    .contentType("text/csv")
                    .contentLength((long) csvContent.getBytes().length)
                    .build();

            s3Client.putObject(request, RequestBody.fromString(csvContent));

            String s3Path = "s3://" + bucket + "/" + s3Key;
            log.info("[S3_UPLOAD_SUCCESS] path={} size_bytes={}", s3Path, csvContent.length());
            return Result.success(s3Path);

        } catch (S3Exception e) {
            log.error("[S3_UPLOAD_FAILURE] date={} key={} error={}", exportDate, s3Key, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.S3_UPLOAD_FAILED,
                    "Ошибка загрузки в S3: " + e.getMessage()));
        } catch (Exception e) {
            log.error("[S3_CONNECTION_FAILURE] date={} error={}", exportDate, e.getMessage());
            return Result.failure(AppError.of(ErrorCode.S3_CONNECTION_FAILED, e));
        }
    }

    private String buildCsv(List<DailyMetric> metrics) {
        StringWriter sw = new StringWriter();
        try (CSVWriter writer = new CSVWriter(sw)) {
            writer.writeNext(CSV_HEADER);
            for (DailyMetric m : metrics) {
                writer.writeNext(new String[]{
                        m.metricDate().toString(),
                        m.metricName(),
                        m.dimensionKey(),
                        String.valueOf(m.metricValue()),
                        m.computedAt().toString()
                });
            }
        } catch (Exception e) {
            log.warn("[CSV_BUILD_WARN] {}", e.getMessage());
        }
        return sw.toString();
    }
}
