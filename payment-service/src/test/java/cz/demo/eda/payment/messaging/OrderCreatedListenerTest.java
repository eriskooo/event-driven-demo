package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.inbox.InboxRepository;
import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Tracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderCreatedListenerTest {

    @Mock
    private InboxRepository inbox;
    @Mock
    private Acknowledgment ack;

    private SimpleMeterRegistry registry;
    private OrderCreatedListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new OrderCreatedListener(inbox, JsonMapper.builder().build(), new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Objednávku uloží do inboxu s correlationId z hlavičky a potvrdí offset")
    void should_storeAndAck_whenFirstDelivery() {
        var event = order();
        var record = record(event);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, "corr-h".getBytes(StandardCharsets.UTF_8));
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(true);

        listener.onOrderCreated(record, ack);

        verify(inbox).store(eq(event.eventId()), eq("orders.created"), eq("o-1"), contains("\"orderId\":\"o-1\""),
                eq("corr-h"));
        verify(ack).acknowledge();
        assertThat(consumed(MessagingMetrics.OUTCOME_STORED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Duplicitní OrderCreated jen potvrdí a započítá jako duplicate")
    void should_ackAndCountDuplicate_whenAlreadyInInbox() {
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(false);

        listener.onOrderCreated(record(order()), ack);

        verify(ack).acknowledge();
        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez hlavičky použije correlationId z těla události")
    void should_useBodyCorrelationId_whenHeaderMissing() {
        var event = order();
        when(inbox.store(any(), anyString(), anyString(), anyString(), any())).thenReturn(true);

        listener.onOrderCreated(record(event), ack);

        verify(inbox).store(eq(event.eventId()), anyString(), anyString(), anyString(), eq("corr-body"));
    }

    @Test
    @DisplayName("Při chybě ukládání offset nepotvrdí – rozhodne Kafka error handler")
    void should_notAck_whenInboxStoreFails() {
        when(inbox.store(any(), anyString(), anyString(), anyString(), any()))
                .thenThrow(new IllegalStateException("db down"));

        assertThatIllegalStateException().isThrownBy(() -> listener.onOrderCreated(record(order()), ack));

        verify(ack, never()).acknowledge();
    }

    private double consumed(String outcome) {
        var counter = registry.find(MessagingMetrics.CONSUMED).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static OrderCreated order() {
        return OrderCreated.of("corr-body", "o-1", "cust", BigDecimal.TEN, "CZK");
    }

    private static ConsumerRecord<String, OrderCreated> record(OrderCreated event) {
        return new ConsumerRecord<>("orders.created", 0, 0, event.orderId(), event);
    }
}
