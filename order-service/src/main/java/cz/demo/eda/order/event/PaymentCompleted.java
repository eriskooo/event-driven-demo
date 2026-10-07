package cz.demo.eda.order.event;

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
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Vytvoří novou událost s vygenerovaným eventId a aktuálním časem. */
    public static PaymentCompleted of(String correlationId, String orderId, String paymentId,
                                      BigDecimal amount) {
        return new PaymentCompleted(UUID.randomUUID(), Instant.now(), correlationId, orderId,
                paymentId, amount);
    }
}
