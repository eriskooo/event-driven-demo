package cz.demo.eda.order.messaging;

import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.inbox.InboxRepository;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Tracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultListenerTest {

    @Mock
    private InboxRepository inbox;

    private SimpleMeterRegistry registry;
    private PaymentResultListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new PaymentResultListener(inbox, JsonMapper.builder().build(), new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Výsledek platby uloží do inboxu s correlationId z hlavičky a typem v payloadu")
    void should_storeInInbox_whenFirstDelivery() {
        var event = PaymentCompleted.of("corr-body", "o-1", "p-1", BigDecimal.ONE);
        var record = record(event);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, "corr-header".getBytes(StandardCharsets.UTF_8));
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(true);

        listener.onPaymentResult(record);

        verify(inbox).store(eq(event.eventId()), eq("payments.result"), eq("o-1"),
                contains("\"type\":\"PaymentCompleted\""), eq("corr-header"));
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez hlavičky použije correlationId z těla události")
    void should_useBodyCorrelationId_whenHeaderMissing() {
        var event = PaymentCompleted.of("corr-body", "o-1", "p-1", BigDecimal.ONE);
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(true);

        listener.onPaymentResult(record(event));

        verify(inbox).store(eq(event.eventId()), anyString(), anyString(), anyString(), eq("corr-body"));
    }

    @Test
    @DisplayName("Duplicitní eventId započítá jako duplicate")
    void should_countDuplicate_whenAlreadyInInbox() {
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(false);

        listener.onPaymentResult(record(PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE)));

        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isZero();
    }

    @Test
    @DisplayName("Chyba při ukládání se propaguje, aby Kafka zprávu doručila znovu")
    void should_propagate_whenInboxStoreFails() {
        when(inbox.store(any(), anyString(), anyString(), anyString(), any()))
                .thenThrow(new IllegalStateException("db down"));

        assertThatIllegalStateException().isThrownBy(() ->
                listener.onPaymentResult(record(PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE))));
    }

    private double consumed(String outcome) {
        var counter = registry.find(MessagingMetrics.CONSUMED).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static ConsumerRecord<String, PaymentResult> record(PaymentResult event) {
        return new ConsumerRecord<>("payments.result", 0, 0, event.orderId(), event);
    }
}
