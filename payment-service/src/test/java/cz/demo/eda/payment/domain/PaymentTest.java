package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class PaymentTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");
    private static final OrderCreated ORDER = OrderCreated.of("c", "o-1", "cust", new BigDecimal("12.00"), "EUR");

    @Test
    @DisplayName("Úspěšná platba převezme paymentId z výsledku a částku z objednávky")
    void should_buildCompletedPayment_whenResultCompleted() {
        var payment = Payment.of(ORDER, PaymentCompleted.of("c", "o-1", "p-1", ORDER.amount()), NOW);

        assertThat(payment.id()).isEqualTo("p-1");
        assertThat(payment.status()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(payment.amount()).isEqualByComparingTo("12.00");
        assertThat(payment.currency()).isEqualTo("EUR");
        assertThat(payment.failureReason()).isNull();
        assertThat(payment.createdAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("Zamítnutá platba dostane nové ID a důvod zamítnutí")
    void should_buildFailedPayment_whenResultFailed() {
        var payment = Payment.of(ORDER, PaymentFailed.of("c", "o-1", "declined"), NOW);

        assertThat(payment.id()).isNotBlank();
        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.failureReason()).isEqualTo("declined");
    }

    @Test
    @DisplayName("Odmítne platbu bez ID objednávky")
    void should_throw_whenOrderIdIsNull() {
        assertThatNullPointerException().isThrownBy(() ->
                new Payment("p", null, PaymentStatus.COMPLETED, BigDecimal.ONE, "CZK", null, NOW));
    }
}
