package cz.demo.eda.order.messaging;

import cz.demo.eda.order.event.OrderCommand;
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
 * Přijímá příkazy orchestrátoru a jen je uloží do inboxu; offset se commituje (AckMode.RECORD) až po
 * úspěšném uložení. Samotné zpracování s retry obstará InboxProcessor.
 */
@Component
public class OrderCommandListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public OrderCommandListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží příkaz do inboxu; duplicitní eventId (opakovaný job v Zeebe) přeskočí. */
    @KafkaListener(id = "order-command-listener", idIsGroup = false, topics = Topics.ORDERS_COMMANDS)
    public void onOrderCommand(ConsumerRecord<String, OrderCommand> record) {
        OrderCommand command = record.value();
        String type = command.getClass().getSimpleName();
        boolean stored = inbox.store(InboxEntry.received(command.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(command), correlationId(record, command), clock.instant()));
        if (!stored) {
            log.info("Duplicate {} {} for order {} skipped", type, command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
            return;
        }
        log.info("Received {} {} for order {} stored in inbox", type, command.eventId(), command.orderId());
        metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
    }

    private static String correlationId(ConsumerRecord<?, ?> record, OrderCommand command) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : command.correlationId();
    }
}
