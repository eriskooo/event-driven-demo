package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Příkaz pro payment-service: proveď platbu za objednávku. */
public record ProcessPayment(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        BigDecimal amount,
        String currency) implements DomainEvent {

    public ProcessPayment {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
    }
}
