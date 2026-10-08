package cz.demo.eda.order.domain;

import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.event.CancelOrder;
import cz.demo.eda.order.event.ConfirmOrder;
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

        Order order = service.createOrder("c-1", new BigDecimal("99.90"), "CZK", "corr-1");

        ArgumentCaptor<OrderCreated> captor = ArgumentCaptor.forClass(OrderCreated.class);
        verify(outbox).publish(eq("orders.created"), captor.capture());
        OrderCreated event = captor.getValue();
        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
        assertThat(order.createdAt()).isEqualTo(NOW);
        assertThat(event.orderId()).isEqualTo(order.id());
        assertThat(event.correlationId()).isEqualTo("corr-1");
        assertThat(event.amount()).isEqualByComparingTo("99.90");
    }

    @Test
    @DisplayName("ConfirmOrder převede objednávku do stavu PAID")
    void should_markPaid_whenConfirmOrder() {
        Order order = pending();
        when(repository.findForUpdate("o-1")).thenReturn(Optional.of(order));

        service.applyCommand(ConfirmOrder.of("corr", "o-1", "p-1"));

        assertThat(order.status()).isEqualTo(OrderStatus.PAID);
        assertThat(order.paymentId()).isEqualTo("p-1");
        assertThat(order.updatedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("CancelOrder převede objednávku do stavu PAYMENT_FAILED s důvodem")
    void should_markFailed_whenCancelOrder() {
        Order order = pending();
        when(repository.findForUpdate("o-1")).thenReturn(Optional.of(order));

        service.applyCommand(CancelOrder.of("corr", "o-1", "declined"));

        assertThat(order.status()).isEqualTo(OrderStatus.PAYMENT_FAILED);
        assertThat(order.failureReason()).isEqualTo("declined");
    }

    @Test
    @DisplayName("Finální stav se dalším příkazem nezmění")
    void should_keepFinalState_whenSecondCommandArrives() {
        Order order = pending();
        order.markPaid("p-1", NOW);
        when(repository.findForUpdate("o-1")).thenReturn(Optional.of(order));

        service.applyCommand(CancelOrder.of("corr", "o-1", "late"));

        assertThat(order.status()).isEqualTo(OrderStatus.PAID);
        assertThat(order.failureReason()).isNull();
    }

    @Test
    @DisplayName("Příkaz pro neznámou objednávku vrátí prázdno")
    void should_returnEmpty_whenOrderUnknown() {
        when(repository.findForUpdate("unknown")).thenReturn(Optional.empty());

        assertThat(service.applyCommand(CancelOrder.of("corr", "unknown", "x"))).isEmpty();
    }

    @Test
    @DisplayName("Hledání podle ID deleguje na repozitář")
    void should_delegateFind_whenFindById() {
        when(repository.findById("o-1")).thenReturn(Optional.of(pending()));

        assertThat(service.findById("o-1")).isPresent();
    }

    private static Order pending() {
        return Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", NOW.minusSeconds(60));
    }
}
