package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.event.ProcessPayment;
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
 * Přijímá příkazy k platbě od orchestrátoru a jen je uloží do inboxu. Offset potvrdí ručně
 * (AckMode.MANUAL_IMMEDIATE) až po úspěšném uložení; samotnou platbu s retry provede InboxProcessor.
 */
@Component
public class ProcessPaymentListener {

    private static final Logger log = LoggerFactory.getLogger(ProcessPaymentListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public ProcessPaymentListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží příkaz do inboxu a potvrdí offset; duplicitní eventId (opakovaný job v Zeebe) jen potvrdí. */
    @KafkaListener(id = "process-payment-listener", idIsGroup = false, topics = Topics.PAYMENTS_COMMANDS)
    public void onProcessPayment(ConsumerRecord<String, ProcessPayment> record, Acknowledgment ack) {
        ProcessPayment command = record.value();
        boolean stored = inbox.store(InboxEntry.received(command.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(command), correlationId(record, command), clock.instant()));
        if (stored) {
            log.info("ProcessPayment {} for order {} stored in inbox", command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
        } else {
            log.info("Duplicate ProcessPayment {} for order {} skipped", command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
        }
        ack.acknowledge();
    }

    private static String correlationId(ConsumerRecord<?, ?> record, ProcessPayment command) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : command.correlationId();
    }
}
