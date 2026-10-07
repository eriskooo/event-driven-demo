package cz.demo.eda.order.support;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdRecordInterceptorTest {

    private final CorrelationIdRecordInterceptor interceptor = new CorrelationIdRecordInterceptor();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("Vloží correlationId z hlavičky do MDC a po zpracování ho odstraní")
    void should_putAndRemoveMdc_whenHeaderPresent() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<Object, Object>("t", 0, 0, "k", "v");
        record.headers().add(Tracing.CORRELATION_ID_HEADER, "corr-9".getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<Object, Object> returned = interceptor.intercept(record, null);

        assertThat(returned).isSameAs(record);
        assertThat(MDC.get(Tracing.CORRELATION_ID_MDC_KEY)).isEqualTo("corr-9");
        interceptor.afterRecord(record, null);
        assertThat(MDC.get(Tracing.CORRELATION_ID_MDC_KEY)).isNull();
    }

    @Test
    @DisplayName("Bez hlavičky MDC nezmění")
    void should_leaveMdcEmpty_whenHeaderMissing() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<Object, Object>("t", 0, 0, "k", "v");

        interceptor.intercept(record, null);

        assertThat(MDC.get(Tracing.CORRELATION_ID_MDC_KEY)).isNull();
    }
}
