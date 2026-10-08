package cz.demo.eda.order.event;

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

    /** Vytvoří nový příkaz s vygenerovaným eventId a aktuálním časem. */
    public static ConfirmOrder of(String correlationId, String orderId, String paymentId) {
        return new ConfirmOrder(UUID.randomUUID(), Instant.now(), correlationId, orderId, paymentId);
    }
}
