package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.support.ProcessedEventStore;
import cz.demo.eda.payment.domain.PaymentSimulator;
import cz.demo.eda.payment.support.MessagingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Konzumuje vytvořené objednávky s ručním potvrzením (AckMode.MANUAL_IMMEDIATE):
 * offset se commitne až po odeslání výsledku platby.
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final PaymentSimulator simulator;
    private final PaymentResultPublisher publisher;
    private final ProcessedEventStore processedEvents;
    private final MessagingMetrics metrics;

    public OrderCreatedListener(PaymentSimulator simulator, PaymentResultPublisher publisher,
                                ProcessedEventStore processedEvents, MessagingMetrics metrics) {
        this.simulator = simulator;
        this.publisher = publisher;
        this.processedEvents = processedEvents;
        this.metrics = metrics;
    }

    /** Zpracuje objednávku právě jednou; výjimka spouští retry a po vyčerpání pokusů DLT. */
    @KafkaListener(id = "order-created-listener", idIsGroup = false, topics = Topics.ORDERS_CREATED)
    public void onOrderCreated(OrderCreated event, Acknowledgment ack) {
        if (processedEvents.isProcessed(event.eventId())) {
            log.info("Duplicate OrderCreated {} for order {} skipped", event.eventId(), event.orderId());
            metrics.consumed(Topics.ORDERS_CREATED, MessagingMetrics.OUTCOME_DUPLICATE);
            ack.acknowledge();
            return;
        }
        log.info("Processing payment for order {} amount {} {}", event.orderId(), event.amount(), event.currency());
        var result = simulator.process(event);
        publisher.publish(result);
        processedEvents.markProcessed(event.eventId());
        metrics.consumed(Topics.ORDERS_CREATED, MessagingMetrics.OUTCOME_PROCESSED);
        ack.acknowledge();
    }
}
