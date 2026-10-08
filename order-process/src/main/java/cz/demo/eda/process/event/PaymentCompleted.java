package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba proběhla úspěšně. */
public record PaymentCompleted(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String paymentId,
        BigDecimal amount) implements PaymentResult {

    public PaymentCompleted {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
    }
}
