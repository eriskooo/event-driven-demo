package cz.demo.eda.order.support;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/** Převezme correlationId z hlavičky (nebo vygeneruje nové) a vloží ho do MDC pro celý request. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        var header = request.getHeader(Tracing.CORRELATION_ID_HEADER);
        var correlationId = header == null || header.isBlank() ? UUID.randomUUID().toString() : header;
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, correlationId);
        response.setHeader(Tracing.CORRELATION_ID_HEADER, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
    }
}
