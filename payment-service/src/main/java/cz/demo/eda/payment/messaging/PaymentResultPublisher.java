package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import cz.demo.eda.payment.event.PaymentResult;
import cz.demo.eda.payment.support.MessagingMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** Publikuje výsledek platby s klíčem orderId. */
@Component
public class PaymentResultPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MessagingMetrics metrics;

    public PaymentResultPublisher(KafkaTemplate<String, Object> kafkaTemplate, MessagingMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    /**
     * Synchronně odešle výsledek. Čeká na potvrzení brokeru, aby se offset vstupní zprávy
     * potvrdil až po úspěšném odeslání – jinak by výsledek mohl tiše zmizet.
     */
    public SendResult<String, Object> publish(PaymentResult result) {
        var record = new ProducerRecord<String, Object>(Topics.PAYMENTS_RESULT, result.orderId(), result);
        if (result.correlationId() != null) {
            record.headers().add(Tracing.CORRELATION_ID_HEADER, result.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        var sent = kafkaTemplate.send(record).join();
        metrics.produced(Topics.PAYMENTS_RESULT);
        log.info("Published {} {} for order {} to partition {} offset {}", result.getClass().getSimpleName(),
                result.eventId(), result.orderId(), sent.getRecordMetadata().partition(), sent.getRecordMetadata().offset());
        return sent;
    }
}
