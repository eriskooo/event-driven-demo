package cz.demo.eda.order.messaging;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.support.ProcessedEventStore;
import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.support.MessagingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Konzumuje výsledky plateb; offset se commituje po každém záznamu (AckMode.RECORD). */
@Component
public class PaymentResultListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultListener.class);

    private final OrderService orderService;
    private final ProcessedEventStore processedEvents;
    private final MessagingMetrics metrics;

    public PaymentResultListener(OrderService orderService, ProcessedEventStore processedEvents, MessagingMetrics metrics) {
        this.orderService = orderService;
        this.processedEvents = processedEvents;
        this.metrics = metrics;
    }

    /** Zpracuje výsledek platby právě jednou – duplicity podle eventId přeskočí. */
    @KafkaListener(id = "payment-result-listener", idIsGroup = false, topics = Topics.PAYMENTS_RESULT)
    public void onPaymentResult(PaymentResult result) {
        var type = result.getClass().getSimpleName();
        if (processedEvents.isProcessed(result.eventId())) {
            log.info("Duplicate {} {} for order {} skipped", type, result.eventId(), result.orderId());
            metrics.consumed(Topics.PAYMENTS_RESULT, MessagingMetrics.OUTCOME_DUPLICATE);
            return;
        }
        log.info("Received {} {} for order {}", type, result.eventId(), result.orderId());
        orderService.applyPaymentResult(result);
        processedEvents.markProcessed(result.eventId());
        metrics.consumed(Topics.PAYMENTS_RESULT, MessagingMetrics.OUTCOME_PROCESSED);
    }
}
