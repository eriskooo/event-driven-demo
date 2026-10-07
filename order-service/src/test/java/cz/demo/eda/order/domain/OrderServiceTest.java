package cz.demo.eda.order.domain;

import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.event.PaymentFailed;
import cz.demo.eda.order.messaging.OrderEventPublisher;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

    @Mock
    private OrderEventPublisher publisher;

    private OrderRepository repository;
    private OrderService service;

    @BeforeEach
    void setUp() {
        repository = new OrderRepository();
        service = new OrderService(repository, publisher, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Založení objednávky ji uloží a publikuje OrderCreated se stejným ID a correlationId")
    void should_saveAndPublish_whenOrderCreated() {
        var order = service.createOrder("c-1", new BigDecimal("99.90"), "CZK", "corr-1");

        var captor = ArgumentCaptor.forClass(OrderCreated.class);
        verify(publisher).publish(captor.capture());
        var event = captor.getValue();
        assertThat(repository.findById(order.id())).contains(order);
        assertThat(event.orderId()).isEqualTo(order.id());
        assertThat(event.correlationId()).isEqualTo("corr-1");
        assertThat(event.amount()).isEqualByComparingTo("99.90");
        assertThat(order.createdAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("PaymentCompleted převede objednávku do stavu PAID")
    void should_markPaid_whenPaymentCompleted() {
        var order = service.createOrder("c-1", BigDecimal.TEN, "CZK", "corr");

        var result = service.applyPaymentResult(PaymentCompleted.of("corr", order.id(), "p-1", BigDecimal.TEN));

        assertThat(result).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("PaymentFailed převede objednávku do stavu PAYMENT_FAILED s důvodem")
    void should_markFailed_whenPaymentFailed() {
        var order = service.createOrder("c-1", BigDecimal.TEN, "CZK", "corr");

        var result = service.applyPaymentResult(PaymentFailed.of("corr", order.id(), "declined"));

        assertThat(result).get().satisfies(o -> {
            assertThat(o.status()).isEqualTo(OrderStatus.PAYMENT_FAILED);
            assertThat(o.failureReason()).isEqualTo("declined");
        });
    }

    @Test
    @DisplayName("Finální stav se dalším výsledkem platby nezmění")
    void should_keepFinalState_whenSecondResultArrives() {
        var order = service.createOrder("c-1", BigDecimal.TEN, "CZK", "corr");
        service.applyPaymentResult(PaymentCompleted.of("corr", order.id(), "p-1", BigDecimal.TEN));

        var result = service.applyPaymentResult(PaymentFailed.of("corr", order.id(), "late"));

        assertThat(result).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("Výsledek platby pro neznámou objednávku vrátí prázdno")
    void should_returnEmpty_whenOrderUnknown() {
        assertThat(service.applyPaymentResult(PaymentFailed.of("corr", "unknown", "x"))).isEmpty();
    }

    @Test
    @DisplayName("Hledání podle null ID vrátí prázdno")
    void should_returnEmpty_whenFindByNullId() {
        assertThat(service.findById(null)).isEmpty();
    }
}
