package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.support.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Čte DLT jen pro zviditelnění v logu (a tedy v Kibaně). Vlastní consumer group,
 * aby neovlivňoval hlavní listener; hodnota se čte jako text, protože může jít o nevalidní JSON.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);
    private static final String DLT_TOPIC = Topics.ORDERS_CREATED + Topics.DLT_SUFFIX;

    /** Zaloguje zprávu z DLT včetně důvodu selhání z hlaviček přidaných recovererem. */
    @KafkaListener(id = "order-created-dlt-listener", idIsGroup = false, topics = DLT_TOPIC, groupId = "payment-service-dlt",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        var headers = record.headers();
        log.error("Dead letter received: key={} originalTopic={} originalOffset={} exception={} message={} payload={}",
                record.key(), header(headers, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                originalOffset(headers), header(headers, KafkaHeaders.DLT_EXCEPTION_FQCN),
                header(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE), record.value());
        ack.acknowledge();
    }

    static String header(Headers headers, String name) {
        var header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Long originalOffset(Headers headers) {
        var header = headers.lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
        // Recoverer ukládá offset jako 8bajtový long, ne jako text.
        return header == null || header.value().length != Long.BYTES
                ? null
                : ByteBuffer.wrap(header.value()).getLong();
    }
}
