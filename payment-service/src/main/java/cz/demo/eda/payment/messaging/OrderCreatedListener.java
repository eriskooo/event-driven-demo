package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.inbox.InboxEntry;
import cz.demo.eda.payment.inbox.InboxService;
import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

/**
 * Přijímá vytvořené objednávky a jen je uloží do inboxu. Offset potvrdí ručně (AckMode.MANUAL_IMMEDIATE)
 * až po úspěšném uložení; samotnou platbu s retry provede InboxProcessor.
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public OrderCreatedListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží objednávku do inboxu a potvrdí offset; duplicitní eventId jen potvrdí. */
    @KafkaListener(id = "order-created-listener", idIsGroup = false, topics = Topics.ORDERS_CREATED)
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record, Acknowledgment ack) {
        OrderCreated event = record.value();
        boolean stored = inbox.store(InboxEntry.received(event.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(event), correlationId(record, event), clock.instant()));
        if (stored) {
            log.info("OrderCreated {} for order {} stored in inbox", event.eventId(), event.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
        } else {
            log.info("Duplicate OrderCreated {} for order {} skipped", event.eventId(), event.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
        }
        ack.acknowledge();
    }

    private static String correlationId(ConsumerRecord<?, ?> record, OrderCreated event) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : event.correlationId();
    }
}
