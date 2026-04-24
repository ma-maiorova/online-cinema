package com.cinema.producer.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Servlet-фильтр: устанавливает correlationId в MDC для каждого HTTP-запроса.
 *
 * <p>Значение берётся из заголовка {@code X-Correlation-Id}, или генерируется новый UUID.
 * Добавляется в ответный заголовок, чтобы клиент мог коррелировать запросы с логами.
 *
 * <p>MDC-ключ {@code correlationId} используется в паттерне логирования (application.yml).
 */
@Component
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        String correlationId = resolveCorrelationId(request);
        MDC.put(MDC_KEY, correlationId);
        response.setHeader(CORRELATION_ID_HEADER, correlationId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }

    private String resolveCorrelationId(HttpServletRequest request) {
        String fromHeader = request.getHeader(CORRELATION_ID_HEADER);
        if (fromHeader != null && !fromHeader.isBlank()) {
            return fromHeader;
        }
        return UUID.randomUUID().toString();
    }
}
