package cz.demo.eda.order.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** Neměnný snapshot objednávky; změny stavu vrací novou instanci. */
public record Order(
        String id,
        String customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status,
        String paymentId,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {

    public Order {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(amount, "amount");
    }

    /** Založí novou objednávku čekající na platbu. */
    public static Order create(String id, String customerId, BigDecimal amount, String currency, Instant now) {
        return new Order(id, customerId, amount, currency, OrderStatus.PENDING_PAYMENT, null, null, now, now);
    }

    /** Označí objednávku jako zaplacenou. */
    public Order markPaid(String paymentId, Instant now) {
        return new Order(id, customerId, amount, currency, OrderStatus.PAID, paymentId, null, createdAt, now);
    }

    /** Označí objednávku jako neúspěšně zaplacenou s uvedeným důvodem. */
    public Order markPaymentFailed(String reason, Instant now) {
        return new Order(id, customerId, amount, currency, OrderStatus.PAYMENT_FAILED, null, reason, createdAt, now);
    }
}
