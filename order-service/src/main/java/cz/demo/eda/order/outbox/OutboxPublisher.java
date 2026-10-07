package cz.demo.eda.order.outbox;

import cz.demo.eda.order.event.DomainEvent;
import cz.demo.eda.order.inbox.InboxMessage;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Zapisuje odchozí zprávy do outboxu – samotné odeslání do Kafky obstará {@link OutboxRelay}. */
@Component
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

    public OutboxPublisher(OutboxRepository repository, JsonMapper jsonMapper) {
        this.repository = repository;
        this.jsonMapper = jsonMapper;
    }

    /**
     * Zařadí doménovou událost k odeslání s klíčem orderId.
     * MANDATORY: bez okolní transakce by se ztratila atomicita s doménovou změnou.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String topic, DomainEvent event) {
        repository.insert(event.eventId(), topic, event.orderId(), jsonMapper.writeValueAsString(event),
                correlationHeader(event.correlationId()));
        log.debug("{} {} for order {} stored in outbox", event.getClass().getSimpleName(), event.eventId(),
                event.orderId());
    }

    /** Zařadí zprávu z inboxu, kterou se nepodařilo zpracovat, do &lt;topic&gt;.DLT s důvodem selhání. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void publishDeadLetter(InboxMessage message, int attempts, Exception cause) {
        var headers = new HashMap<>(correlationHeader(message.correlationId()));
        headers.put(DLT_ORIGINAL_TOPIC, message.topic());
        headers.put(DLT_EXCEPTION_FQCN, cause.getClass().getName());
        headers.put(DLT_EXCEPTION_MESSAGE, truncate(Objects.toString(cause.getMessage(), "")));
        headers.put(DLT_ATTEMPTS, Integer.toString(attempts));
        repository.insert(message.eventId(), Topics.dltOf(message.topic()), message.key(), message.payload(), headers);
    }

    private static Map<String, String> correlationHeader(String correlationId) {
        return correlationId == null ? Map.of() : Map.of(Tracing.CORRELATION_ID_HEADER, correlationId);
    }

    private static String truncate(String value) {
        return value.length() <= MAX_HEADER_LENGTH ? value : value.substring(0, MAX_HEADER_LENGTH);
    }
}
