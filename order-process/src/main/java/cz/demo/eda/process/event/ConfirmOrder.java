package cz.demo.eda.process.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba proběhla – objednávku potvrdit. */
public record ConfirmOrder(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String paymentId) implements OrderCommand {

    public ConfirmOrder {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }
}
