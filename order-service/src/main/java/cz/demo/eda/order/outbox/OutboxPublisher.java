package cz.demo.eda.order.outbox;

import cz.demo.eda.order.event.DomainEvent;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Zapisuje odchozí zprávy do outboxu – samotné odeslání do Kafky obstará {@link OutboxService}.
 * MANDATORY: zápis musí být součástí transakce volajícího servisu, jinak by se ztratila atomicita
 * s doménovou změnou.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class OutboxPublisher {

    /** Hlavičky DLT zprávy z inboxu (analogie kafka_dlt-* hlaviček Spring Kafka). */
    public static final String DLT_ORIGINAL_TOPIC = "eda-dlt-original-topic";
    public static final String DLT_EXCEPTION_FQCN = "eda-dlt-exception-fqcn";
    public static final String DLT_EXCEPTION_MESSAGE = "eda-dlt-exception-message";
    public static final String DLT_ATTEMPTS = "eda-dlt-attempts";

    private static final int MAX_HEADER_LENGTH = 1000;
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository repository;
    private final JsonMapper jsonMapper;
    private final Clock clock;

    public OutboxPublisher(OutboxRepository repository, JsonMapper jsonMapper, Clock clock) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
        this.clock = clock;
    }

    /** Zařadí doménovou událost k odeslání s klíčem orderId. */
    public void publish(String topic, DomainEvent event) {
        repository.save(OutboxEntry.pending(event.eventId(), topic, event.orderId(), jsonMapper.writeValueAsString(event),
                correlationHeader(event.correlationId()), clock.instant()));
        log.debug("{} {} for order {} stored in outbox", event.getClass().getSimpleName(), event.eventId(),
                event.orderId());
    }

    /**
     * Zařadí zprávu z inboxu, kterou se nepodařilo zpracovat, do &lt;topic&gt;.DLT s důvodem selhání
     * a počtem pokusů.
     */
    public void publishDeadLetter(InboxEntry entry, Exception cause) {
        Map<String, String> headers = new HashMap<>(correlationHeader(entry.correlationId()));
        headers.put(DLT_ORIGINAL_TOPIC, entry.topic());
        headers.put(DLT_EXCEPTION_FQCN, cause.getClass().getName());
        headers.put(DLT_EXCEPTION_MESSAGE, truncate(Objects.toString(cause.getMessage(), "")));
        headers.put(DLT_ATTEMPTS, Integer.toString(entry.attempts()));
        repository.save(OutboxEntry.pending(entry.eventId(), Topics.dltOf(entry.topic()), entry.messageKey(),
                entry.payload(), headers, clock.instant()));
    }

    private static Map<String, String> correlationHeader(String correlationId) {
        return correlationId == null ? Map.of() : Map.of(Tracing.CORRELATION_ID_HEADER, correlationId);
    }

    private static String truncate(String value) {
        return value.length() <= MAX_HEADER_LENGTH ? value : value.substring(0, MAX_HEADER_LENGTH);
    }
}
