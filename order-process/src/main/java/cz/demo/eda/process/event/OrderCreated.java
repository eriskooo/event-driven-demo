package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Událost order-service o založení objednávky – spouští proces. */
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
        Objects.requireNonNull(orderId, "orderId");
    }
}
