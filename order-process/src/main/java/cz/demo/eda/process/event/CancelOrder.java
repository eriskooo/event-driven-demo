package cz.demo.eda.process.event;

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
}
