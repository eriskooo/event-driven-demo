package cz.demo.eda.order.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class OrderTest {

    private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant T1 = Instant.parse("2026-01-01T10:00:05Z");

    @Test
    @DisplayName("Nová objednávka čeká na platbu a je nová pro Spring Data")
    void should_bePendingPaymentAndNew_whenCreated() {
        Order order = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0);

        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
        assertThat(order.createdAt()).isEqualTo(T0).isEqualTo(order.updatedAt());
        assertThat(order.isNew()).isTrue();
        assertThat(order.getId()).isEqualTo("o-1");
    }

    @Test
    @DisplayName("Po zaplacení nese paymentId a nový čas změny")
    void should_bePaid_whenMarkedPaid() {
        Order order = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0);

        order.markPaid("p-1", T1);

        assertThat(order.status()).isEqualTo(OrderStatus.PAID);
        assertThat(order.paymentId()).isEqualTo("p-1");
        assertThat(order.createdAt()).isEqualTo(T0);
        assertThat(order.updatedAt()).isEqualTo(T1);
    }

    @Test
    @DisplayName("Po zamítnutí platby nese důvod selhání a žádné paymentId")
    void should_bePaymentFailed_whenMarkedFailed() {
        Order order = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0);

        order.markPaymentFailed("declined", T1);

        assertThat(order.status()).isEqualTo(OrderStatus.PAYMENT_FAILED);
        assertThat(order.failureReason()).isEqualTo("declined");
        assertThat(order.paymentId()).isNull();
    }

    @Test
    @DisplayName("Odmítne objednávku bez ID nebo částky")
    void should_throw_whenIdOrAmountIsNull() {
        assertThatNullPointerException().isThrownBy(() -> Order.create(null, "c", BigDecimal.ONE, "CZK", T0));
        assertThatNullPointerException().isThrownBy(() -> Order.create("o", "c", null, "CZK", T0));
    }

    @Test
    @DisplayName("Rovnost entit je dána ID")
    void should_beEqual_whenIdsMatch() {
        Order first = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0);
        Order second = Order.create("o-1", "c-2", BigDecimal.ONE, "EUR", T1);

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
        assertThat(first).isNotEqualTo(Order.create("o-2", "c-1", BigDecimal.TEN, "CZK", T0));
    }

    @Test
    @DisplayName("Finální jsou jen stavy PAID a PAYMENT_FAILED")
    void should_reportFinalState_whenStatusIsTerminal() {
        assertThat(OrderStatus.PENDING_PAYMENT.isFinal()).isFalse();
        assertThat(OrderStatus.PAID.isFinal()).isTrue();
        assertThat(OrderStatus.PAYMENT_FAILED.isFinal()).isTrue();
    }
}
