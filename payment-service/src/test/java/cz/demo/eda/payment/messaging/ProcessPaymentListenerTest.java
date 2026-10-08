package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.inbox.InboxEntry;
import cz.demo.eda.payment.inbox.InboxService;
import cz.demo.eda.payment.inbox.InboxStatus;
import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Tracing;
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
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProcessPaymentListenerTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private InboxService inbox;
    @Mock
    private Acknowledgment ack;

    private SimpleMeterRegistry registry;
    private ProcessPaymentListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new ProcessPaymentListener(inbox, JsonMapper.builder().build(), new MessagingMetrics(registry),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Příkaz k platbě uloží do inboxu s correlationId z hlavičky a potvrdí offset")
    void should_storeAndAck_whenFirstDelivery() {
        ProcessPayment event = command();
        ConsumerRecord<String, ProcessPayment> record = record(event);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, "corr-h".getBytes(StandardCharsets.UTF_8));
        when(inbox.store(any())).thenReturn(true);

        listener.onProcessPayment(record, ack);

        InboxEntry stored = captureStored();
        assertThat(stored.eventId()).isEqualTo(event.eventId());
        assertThat(stored.topic()).isEqualTo("payments.commands");
        assertThat(stored.messageKey()).isEqualTo("o-1");
        assertThat(stored.payload()).contains("\"orderId\":\"o-1\"");
        assertThat(stored.correlationId()).isEqualTo("corr-h");
        assertThat(stored.status()).isEqualTo(InboxStatus.PENDING);
        verify(ack).acknowledge();
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Duplicitní ProcessPayment jen potvrdí a započítá jako duplicate")
    void should_ackAndCountDuplicate_whenAlreadyInInbox() {
        when(inbox.store(any())).thenReturn(false);

        listener.onProcessPayment(record(command()), ack);

        verify(ack).acknowledge();
        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez hlavičky použije correlationId z těla události")
    void should_useBodyCorrelationId_whenHeaderMissing() {
        when(inbox.store(any())).thenReturn(true);

        listener.onProcessPayment(record(command()), ack);

        assertThat(captureStored().correlationId()).isEqualTo("corr-body");
    }

    @Test
    @DisplayName("Při chybě ukládání offset nepotvrdí – rozhodne Kafka error handler")
    void should_notAck_whenInboxStoreFails() {
        when(inbox.store(any())).thenThrow(new IllegalStateException("db down"));

        assertThatIllegalStateException().isThrownBy(() -> listener.onProcessPayment(record(command()), ack));

        verify(ack, never()).acknowledge();
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

    private static ProcessPayment command() {
        return ProcessPayment.of("corr-body", "o-1", BigDecimal.TEN, "CZK");
    }

    private static ConsumerRecord<String, ProcessPayment> record(ProcessPayment event) {
        return new ConsumerRecord<>("payments.commands", 0, 0, event.orderId(), event);
    }
}
