package cz.demo.eda.order.messaging;

import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.inbox.InboxService;
import cz.demo.eda.order.inbox.InboxStatus;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Tracing;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultListenerTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private InboxService inbox;

    private SimpleMeterRegistry registry;
    private PaymentResultListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new PaymentResultListener(inbox, JsonMapper.builder().build(), new MessagingMetrics(registry),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Výsledek platby uloží do inboxu s correlationId z hlavičky a typem v payloadu")
    void should_storeInInbox_whenFirstDelivery() {
        PaymentCompleted event = PaymentCompleted.of("corr-body", "o-1", "p-1", BigDecimal.ONE);
        ConsumerRecord<String, PaymentResult> record = record(event);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, "corr-header".getBytes(StandardCharsets.UTF_8));
        when(inbox.store(any())).thenReturn(true);

        listener.onPaymentResult(record);

        InboxEntry stored = captureStored();
        assertThat(stored.eventId()).isEqualTo(event.eventId());
        assertThat(stored.topic()).isEqualTo("payments.result");
        assertThat(stored.messageKey()).isEqualTo("o-1");
        assertThat(stored.payload()).contains("\"type\":\"PaymentCompleted\"");
        assertThat(stored.correlationId()).isEqualTo("corr-header");
        assertThat(stored.status()).isEqualTo(InboxStatus.PENDING);
        assertThat(stored.receivedAt()).isEqualTo(NOW);
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez hlavičky použije correlationId z těla události")
    void should_useBodyCorrelationId_whenHeaderMissing() {
        when(inbox.store(any())).thenReturn(true);

        listener.onPaymentResult(record(PaymentCompleted.of("corr-body", "o-1", "p-1", BigDecimal.ONE)));

        assertThat(captureStored().correlationId()).isEqualTo("corr-body");
    }

    @Test
    @DisplayName("Duplicitní eventId započítá jako duplicate")
    void should_countDuplicate_whenAlreadyInInbox() {
        when(inbox.store(any())).thenReturn(false);

        listener.onPaymentResult(record(PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE)));

        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isZero();
    }

    @Test
    @DisplayName("Chyba při ukládání se propaguje, aby Kafka zprávu doručila znovu")
    void should_propagate_whenInboxStoreFails() {
        when(inbox.store(any())).thenThrow(new IllegalStateException("db down"));

        assertThatIllegalStateException().isThrownBy(() ->
                listener.onPaymentResult(record(PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE))));
    }

    private InboxEntry captureStored() {
        ArgumentCaptor<InboxEntry> captor = ArgumentCaptor.forClass(InboxEntry.class);
        verify(inbox).store(captor.capture());
        return captor.getValue();
    }

    private double consumed(String outcome) {
        Counter counter = registry.find(MessagingMetrics.CONSUMED).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static ConsumerRecord<String, PaymentResult> record(PaymentResult event) {
        return new ConsumerRecord<>("payments.result", 0, 0, event.orderId(), event);
    }
}
