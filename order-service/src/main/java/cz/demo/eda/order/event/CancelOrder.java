package cz.demo.eda.order.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba byla zamítnuta – objednávku zrušit. */
public record CancelOrder(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String reason) implements OrderCommand {

    public CancelOrder {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Vytvoří nový příkaz s vygenerovaným eventId a aktuálním časem. */
    public static CancelOrder of(String correlationId, String orderId, String reason) {
        return new CancelOrder(UUID.randomUUID(), Instant.now(), correlationId, orderId, reason);
    }
}
