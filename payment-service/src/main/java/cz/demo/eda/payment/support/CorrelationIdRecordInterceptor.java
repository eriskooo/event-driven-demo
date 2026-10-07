package cz.demo.eda.payment.support;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** Přenese correlationId z Kafka hlavičky do MDC po dobu zpracování záznamu (včetně retry a DLT logů). */
@Component
public class CorrelationIdRecordInterceptor implements RecordInterceptor<Object, Object> {

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        if (header != null && header.value() != null) {
            MDC.put(Tracing.CORRELATION_ID_MDC_KEY, new String(header.value(), StandardCharsets.UTF_8));
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
    }
}
