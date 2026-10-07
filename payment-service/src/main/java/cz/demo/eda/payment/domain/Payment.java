package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.event.PaymentResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Záznam o zpracované platbě. */
public record Payment(
        String id,
        String orderId,
        PaymentStatus status,
        BigDecimal amount,
        String currency,
        String failureReason,
        Instant createdAt) {

    public Payment {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(status, "status");
    }

    /** Sestaví záznam platby z objednávky a výsledku simulace. */
    public static Payment of(OrderCreated order, PaymentResult result, Instant now) {
        return switch (result) {
            case PaymentCompleted completed -> new Payment(completed.paymentId(), order.orderId(),
                    PaymentStatus.COMPLETED, order.amount(), order.currency(), null, now);
            case PaymentFailed failed -> new Payment(UUID.randomUUID().toString(), order.orderId(),
                    PaymentStatus.FAILED, order.amount(), order.currency(), failed.reason(), now);
        };
    }
}
