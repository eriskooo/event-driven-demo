package cz.demo.eda.process.worker;

import cz.demo.eda.process.support.Tracing;
import org.slf4j.MDC;

/**
 * Drží correlationId v MDC po dobu zpracování jobu, aby ho měly i logy workeru
 * (job worker běží mimo Kafka listener, takže ho žádný interceptor nenastaví).
 */
final class CorrelationScope implements AutoCloseable {

    private final boolean active;

    private CorrelationScope(boolean active) {
        this.active = active;
    }

    /** Vloží correlationId do MDC; pro null nedělá nic. */
    static CorrelationScope open(String correlationId) {
        if (correlationId == null) {
            return new CorrelationScope(false);
        }
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, correlationId);
        return new CorrelationScope(true);
    }

    /** Odstraní correlationId z MDC, pokud ho tento scope vložil. */
    @Override
    public void close() {
        if (active) {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
    }
}
