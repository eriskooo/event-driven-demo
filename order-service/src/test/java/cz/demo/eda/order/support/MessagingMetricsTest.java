package cz.demo.eda.order.support;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessagingMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MessagingMetrics metrics = new MessagingMetrics(registry);

    @Test
    @DisplayName("Předregistrovaný DLT čítač existuje s hodnotou 0")
    void should_exposeZeroCounter_whenDltCounterPreRegistered() {
        metrics.deadLetterCounter("t");

        assertThat(registry.get(MessagingMetrics.DEAD_LETTERED).tag("topic", "t").counter().count()).isZero();
    }

    @Test
    @DisplayName("Čítače se navyšují podle topicu a výsledku")
    void should_incrementPerTag_whenRecorded() {
        metrics.produced("a");
        metrics.produced("a");
        metrics.consumed("a", MessagingMetrics.OUTCOME_STORED);
        metrics.deadLettered("a");

        assertThat(registry.get(MessagingMetrics.PRODUCED).tag("topic", "a").counter().count()).isEqualTo(2.0);
        assertThat(registry.get(MessagingMetrics.CONSUMED).tag("outcome", "stored").counter().count()).isEqualTo(1.0);
        assertThat(registry.get(MessagingMetrics.DEAD_LETTERED).counter().count()).isEqualTo(1.0);
    }
}
