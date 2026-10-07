package cz.demo.eda.payment.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Událost publikovaná order-service po založení objednávky. */
public record OrderCreated(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency) implements DomainEvent {

    public OrderCreated {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
    }

    /** Vytvoří novou událost s vygenerovaným eventId a aktuálním časem. */
    public static OrderCreated of(String correlationId, String orderId, String customerId,
                                  BigDecimal amount, String currency) {
        return new OrderCreated(UUID.randomUUID(), Instant.now(), correlationId, orderId,
                customerId, amount, currency);
    }
}
