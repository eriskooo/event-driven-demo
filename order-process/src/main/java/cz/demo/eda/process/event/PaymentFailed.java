package cz.demo.eda.process.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba byla zamítnuta (business výsledek). */
public record PaymentFailed(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String reason) implements PaymentResult {

    public PaymentFailed {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
    }
}
