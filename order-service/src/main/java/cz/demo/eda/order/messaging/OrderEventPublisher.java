package cz.demo.eda.order.messaging;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.support.MessagingMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/** Publikuje OrderCreated s klíčem orderId, aby události jedné objednávky šly do stejné partition. */
@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessagingMetrics metrics;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, MessagingMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    /** Asynchronně odešle událost; výsledek je zalogován a započítán. */
    public CompletableFuture<SendResult<String, Object>> publish(OrderCreated event) {
        var record = new ProducerRecord<String, Object>(Topics.ORDERS_CREATED, event.orderId(), event);
        if (event.correlationId() != null) {
            record.headers().add(Tracing.CORRELATION_ID_HEADER, event.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        return kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish OrderCreated {} for order {}", event.eventId(), event.orderId(), ex);
                return;
            }
            metrics.produced(Topics.ORDERS_CREATED);
            log.info("Published OrderCreated {} for order {} to partition {} offset {}", event.eventId(),
                    event.orderId(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        });
    }
}
