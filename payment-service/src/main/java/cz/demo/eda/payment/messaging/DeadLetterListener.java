package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.outbox.OutboxPublisher;
import cz.demo.eda.payment.support.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Čte DLT jen pro zviditelnění v logu. Do DLT vedou dvě cesty: Kafka error handler (nečitelná zpráva,
 * hlavičky kafka_dlt-*) a inbox po vyčerpání pokusů (hlavičky eda-dlt-*). Vlastní consumer group,
 * hodnota jako text, protože může jít o nevalidní JSON.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);
    private static final String DLT_TOPIC = Topics.ORDERS_CREATED + Topics.DLT_SUFFIX;

    /** Zaloguje zprávu z DLT včetně důvodu selhání. */
    @KafkaListener(id = "order-created-dlt-listener", idIsGroup = false, topics = DLT_TOPIC, groupId = "payment-service-dlt",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        Headers headers = record.headers();
        log.error("Dead letter received: key={} originalTopic={} exception={} message={} attempts={} payload={}",
                record.key(),
                header(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC, OutboxPublisher.DLT_ORIGINAL_TOPIC),
                header(headers, KafkaHeaders.DLT_EXCEPTION_FQCN, OutboxPublisher.DLT_EXCEPTION_FQCN),
                header(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE, OutboxPublisher.DLT_EXCEPTION_MESSAGE),
                header(headers, OutboxPublisher.DLT_ATTEMPTS), record.value());
        ack.acknowledge();
    }

    /** Vrátí text první nalezené hlavičky z uvedených jmen, nebo null. */
    static String header(Headers headers, String... names) {
        for (String name : names) {
            Header header = headers.lastHeader(name);
            if (header != null && header.value() != null) {
                return new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
