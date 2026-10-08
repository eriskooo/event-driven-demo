package cz.demo.eda.process.process;

import java.math.BigDecimal;

/**
 * Proměnné instance procesu order-fulfillment, jak je čtou job workery.
 * Pole odpovídají proměnným, které zakládají listenery (OrderCreated, PaymentResult).
 */
public record OrderVariables(
        String orderId,
        BigDecimal amount,
        String currency,
        String correlationId,
        String paymentStatus,
        String paymentId,
        String failureReason) {
}
