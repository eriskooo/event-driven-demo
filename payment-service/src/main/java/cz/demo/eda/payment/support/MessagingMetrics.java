package cz.demo.eda.payment.support;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** Business metriky zasílání zpráv; jména jsou shodná v obou službách kvůli společnému dashboardu. */
@Component
public class MessagingMetrics {

    public static final String PRODUCED = "eda.messages.produced";
    public static final String CONSUMED = "eda.messages.consumed";
    public static final String DEAD_LETTERED = "eda.messages.dead.lettered";
    public static final String OUTCOME_PROCESSED = "processed";
    public static final String OUTCOME_DUPLICATE = "duplicate";

    private final MeterRegistry registry;

    public MessagingMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Započítá úspěšně odeslanou zprávu. */
    public void produced(String topic) {
        Counter.builder(PRODUCED).tag("topic", topic).register(registry).increment();
    }

    /** Započítá přijatou zprávu s výsledkem (processed / duplicate). */
    public void consumed(String topic, String outcome) {
        Counter.builder(CONSUMED).tag("topic", topic).tag("outcome", outcome).register(registry).increment();
    }

    /** Započítá zprávu přesunutou do DLT. */
    public void deadLettered(String topic) {
        deadLetterCounter(topic).increment();
    }

    /** Vrátí (a případně zaregistruje) DLT čítač; volá se i předem, aby dashboard ukazoval 0 místo "No data". */
    public Counter deadLetterCounter(String topic) {
        return Counter.builder(DEAD_LETTERED).tag("topic", topic).register(registry);
    }
}
