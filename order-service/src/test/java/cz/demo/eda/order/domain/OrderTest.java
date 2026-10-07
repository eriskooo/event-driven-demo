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
    @DisplayName("Nová objednávka čeká na platbu")
    void should_bePendingPayment_whenCreated() {
        var order = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0);

        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
        assertThat(order.createdAt()).isEqualTo(T0).isEqualTo(order.updatedAt());
    }

    @Test
    @DisplayName("Po zaplacení nese paymentId a nový čas změny")
    void should_bePaid_whenMarkedPaid() {
        var paid = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0).markPaid("p-1", T1);

        assertThat(paid.status()).isEqualTo(OrderStatus.PAID);
        assertThat(paid.paymentId()).isEqualTo("p-1");
        assertThat(paid.createdAt()).isEqualTo(T0);
        assertThat(paid.updatedAt()).isEqualTo(T1);
    }

    @Test
    @DisplayName("Po zamítnutí platby nese důvod selhání")
    void should_bePaymentFailed_whenMarkedFailed() {
        var failed = Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", T0).markPaymentFailed("declined", T1);

        assertThat(failed.status()).isEqualTo(OrderStatus.PAYMENT_FAILED);
        assertThat(failed.failureReason()).isEqualTo("declined");
        assertThat(failed.paymentId()).isNull();
    }

    @Test
    @DisplayName("Odmítne objednávku bez ID")
    void should_throw_whenIdIsNull() {
        assertThatNullPointerException().isThrownBy(() -> Order.create(null, "c", BigDecimal.ONE, "CZK", T0));
    }

    @Test
    @DisplayName("Finální jsou jen stavy PAID a PAYMENT_FAILED")
    void should_reportFinalState_whenStatusIsTerminal() {
        assertThat(OrderStatus.PENDING_PAYMENT.isFinal()).isFalse();
        assertThat(OrderStatus.PAID.isFinal()).isTrue();
        assertThat(OrderStatus.PAYMENT_FAILED.isFinal()).isTrue();
    }
}
