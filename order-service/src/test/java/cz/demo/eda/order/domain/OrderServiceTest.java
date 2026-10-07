package cz.demo.eda.order.domain;

import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.event.PaymentFailed;
import cz.demo.eda.order.outbox.OutboxPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

    @Mock
    private OrderRepository repository;
    @Mock
    private OutboxPublisher outbox;

    private OrderService service;

    @BeforeEach
    void setUp() {
        service = new OrderService(repository, outbox, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Založení objednávky ji uloží a zařadí OrderCreated se stejným ID a correlationId do outboxu")
    void should_saveAndPublishToOutbox_whenOrderCreated() {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        var order = service.createOrder("c-1", new BigDecimal("99.90"), "CZK", "corr-1");

        var captor = ArgumentCaptor.forClass(OrderCreated.class);
        verify(outbox).publish(eq("orders.created"), captor.capture());
        var event = captor.getValue();
        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
        assertThat(order.createdAt()).isEqualTo(NOW);
        assertThat(event.orderId()).isEqualTo(order.id());
        assertThat(event.correlationId()).isEqualTo("corr-1");
        assertThat(event.amount()).isEqualByComparingTo("99.90");
    }

    @Test
    @DisplayName("PaymentCompleted převede objednávku do stavu PAID")
    void should_markPaid_whenPaymentCompleted() {
        givenStoredOrder(pending());

        var result = service.applyPaymentResult(PaymentCompleted.of("corr", "o-1", "p-1", BigDecimal.TEN));

        assertThat(result).get().satisfies(o -> {
            assertThat(o.status()).isEqualTo(OrderStatus.PAID);
            assertThat(o.paymentId()).isEqualTo("p-1");
            assertThat(o.updatedAt()).isEqualTo(NOW);
        });
    }

    @Test
    @DisplayName("PaymentFailed převede objednávku do stavu PAYMENT_FAILED s důvodem")
    void should_markFailed_whenPaymentFailed() {
        givenStoredOrder(pending());

        var result = service.applyPaymentResult(PaymentFailed.of("corr", "o-1", "declined"));

        assertThat(result).get().satisfies(o -> {
            assertThat(o.status()).isEqualTo(OrderStatus.PAYMENT_FAILED);
            assertThat(o.failureReason()).isEqualTo("declined");
        });
    }

    @Test
    @DisplayName("Finální stav se dalším výsledkem platby nezmění")
    void should_keepFinalState_whenSecondResultArrives() {
        givenStoredOrder(pending().markPaid("p-1", NOW));

        var result = service.applyPaymentResult(PaymentFailed.of("corr", "o-1", "late"));

        assertThat(result).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("Výsledek platby pro neznámou objednávku vrátí prázdno")
    void should_returnEmpty_whenOrderUnknown() {
        when(repository.update(eq("unknown"), any())).thenReturn(Optional.empty());

        assertThat(service.applyPaymentResult(PaymentFailed.of("corr", "unknown", "x"))).isEmpty();
    }

    @Test
    @DisplayName("Hledání podle null ID vrátí prázdno")
    void should_returnEmpty_whenFindByNullId() {
        when(repository.findById(null)).thenReturn(Optional.empty());

        assertThat(service.findById(null)).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private void givenStoredOrder(Order stored) {
        when(repository.update(eq(stored.id()), any())).thenAnswer(inv ->
                Optional.of(((UnaryOperator<Order>) inv.getArgument(1)).apply(stored)));
    }

    private static Order pending() {
        return Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", NOW.minusSeconds(60));
    }
}
