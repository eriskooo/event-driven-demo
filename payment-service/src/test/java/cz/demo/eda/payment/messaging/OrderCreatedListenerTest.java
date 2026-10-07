package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.support.ProcessedEventStore;
import cz.demo.eda.payment.domain.PaymentProcessingException;
import cz.demo.eda.payment.domain.PaymentSimulator;
import cz.demo.eda.payment.support.MessagingMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderCreatedListenerTest {

    @Mock
    private PaymentSimulator simulator;
    @Mock
    private PaymentResultPublisher publisher;
    @Mock
    private Acknowledgment ack;

    private SimpleMeterRegistry registry;
    private ProcessedEventStore store;
    private OrderCreatedListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        store = new ProcessedEventStore();
        listener = new OrderCreatedListener(simulator, publisher, store, new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Zpracuje objednávku, publikuje výsledek a potvrdí offset")
    void should_publishResultAndAck_whenFirstDelivery() {
        var event = order();
        var result = PaymentCompleted.of("c", event.orderId(), "p-1", BigDecimal.TEN);
        when(simulator.process(event)).thenReturn(result);

        listener.onOrderCreated(event, ack);

        verify(publisher).publish(result);
        verify(ack).acknowledge();
        assertThat(store.isProcessed(event.eventId())).isTrue();
        assertThat(consumed(MessagingMetrics.OUTCOME_PROCESSED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Duplicitní OrderCreated nepublikuje druhý výsledek, ale offset potvrdí")
    void should_skipAndAck_whenDuplicate() {
        var event = order();
        when(simulator.process(event)).thenReturn(PaymentCompleted.of("c", event.orderId(), "p", BigDecimal.TEN));

        listener.onOrderCreated(event, ack);
        listener.onOrderCreated(event, ack);

        verify(simulator, times(1)).process(event);
        verify(ack, times(2)).acknowledge();
        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Při technické chybě nepotvrdí offset ani neoznačí událost – rozhodne error handler")
    void should_propagateAndNotAck_whenProcessingFails() {
        var event = order();
        when(simulator.process(event)).thenThrow(new PaymentProcessingException("boom"));

        assertThatThrownBy(() -> listener.onOrderCreated(event, ack)).isInstanceOf(PaymentProcessingException.class);

        verify(ack, never()).acknowledge();
        verifyNoInteractions(publisher);
        assertThat(store.isProcessed(event.eventId())).isFalse();
    }

    private double consumed(String outcome) {
        var counter = registry.find(MessagingMetrics.CONSUMED)
                .tags("topic", Topics.ORDERS_CREATED, "outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static OrderCreated order() {
        return OrderCreated.of("c", "o-1", "cust", BigDecimal.TEN, "CZK");
    }
}
