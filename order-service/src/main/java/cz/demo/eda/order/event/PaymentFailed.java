package cz.demo.eda.order.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba byla zamítnuta (business výsledek, ne technická chyba). */
public record PaymentFailed(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String reason) implements PaymentResult {

    public PaymentFailed {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Vytvoří novou událost s vygenerovaným eventId a aktuálním časem. */
    public static PaymentFailed of(String correlationId, String orderId, String reason) {
        return new PaymentFailed(UUID.randomUUID(), Instant.now(), correlationId, orderId, reason);
    }
}
