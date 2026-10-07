package cz.demo.eda.order.messaging;

import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.inbox.InboxService;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

/**
 * Přijímá výsledky plateb a jen je uloží do inboxu; offset se commituje (AckMode.RECORD) až po
 * úspěšném uložení. Samotné zpracování s retry obstará InboxProcessor.
 */
@Component
public class PaymentResultListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public PaymentResultListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží výsledek platby do inboxu; duplicitní eventId přeskočí. */
    @KafkaListener(id = "payment-result-listener", idIsGroup = false, topics = Topics.PAYMENTS_RESULT)
    public void onPaymentResult(ConsumerRecord<String, PaymentResult> record) {
        PaymentResult result = record.value();
        String type = result.getClass().getSimpleName();
        boolean stored = inbox.store(InboxEntry.received(result.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(result), correlationId(record, result), clock.instant()));
        if (!stored) {
            log.info("Duplicate {} {} for order {} skipped", type, result.eventId(), result.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
            return;
        }
        log.info("Received {} {} for order {} stored in inbox", type, result.eventId(), result.orderId());
        metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
    }

    private static String correlationId(ConsumerRecord<?, ?> record, PaymentResult result) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : result.correlationId();
    }
}
