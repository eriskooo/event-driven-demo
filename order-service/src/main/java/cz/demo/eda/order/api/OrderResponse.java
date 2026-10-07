package cz.demo.eda.order.api;

import cz.demo.eda.order.domain.Order;
import cz.demo.eda.order.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;

/** Veřejná reprezentace objednávky v REST API. */
public record OrderResponse(
        String id,
        String customerId,
        BigDecimal amount,
        String currency,
        OrderStatus status,
        String paymentId,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {

    /** Převede doménovou objednávku na DTO. */
    public static OrderResponse from(Order order) {
        return new OrderResponse(order.id(), order.customerId(), order.amount(), order.currency(), order.status(),
                order.paymentId(), order.failureReason(), order.createdAt(), order.updatedAt());
    }
}
