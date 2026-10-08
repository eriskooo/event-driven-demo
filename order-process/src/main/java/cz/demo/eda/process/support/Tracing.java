package cz.demo.eda.process.support;

/** Klíče pro propagaci correlationId přes HTTP, Kafka hlavičky a MDC. */
public final class Tracing {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";

    private Tracing() {
    }
}
