package cz.demo.eda.order.domain;

import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.event.PaymentFailed;
import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

/** Doménová logika objednávek: založení a reakce na výsledek platby. */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository repository;
    private final OutboxPublisher outbox;
    private final Clock clock;

    public OrderService(OrderRepository repository, OutboxPublisher outbox, Clock clock) {
        this.repository = repository;
        this.outbox = outbox;
        this.clock = clock;
    }

    /** Založí objednávku a ve stejné transakci zařadí OrderCreated do outboxu. */
    @Transactional
    public Order createOrder(String customerId, BigDecimal amount, String currency, String correlationId) {
        var order = repository.save(Order.create(UUID.randomUUID().toString(), customerId, amount, currency, clock.instant()));
        outbox.publish(Topics.ORDERS_CREATED, OrderCreated.of(correlationId, order.id(), customerId, amount, currency));
        log.info("Order {} created for customer {} amount {} {}", order.id(), customerId, amount, currency);
        return order;
    }

    /** Najde objednávku podle ID. */
    @Transactional(readOnly = true)
    public Optional<Order> findById(String id) {
        return repository.findById(id);
    }

    /**
     * Promítne výsledek platby do stavu objednávky; vrací prázdno pro neznámou objednávku.
     * Volá ho inbox processor ve své transakci (deduplikaci už zajistil inbox).
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public Optional<Order> applyPaymentResult(PaymentResult result) {
        var updated = repository.update(result.orderId(), order -> transition(order, result));
        if (updated.isEmpty()) {
            // Výsledek pro neznámou objednávku retry nespraví – jen varování, zpráva se označí jako zpracovaná.
            log.warn("Payment result {} for unknown order {} ignored", result.eventId(), result.orderId());
        }
        return updated;
    }

    private Order transition(Order order, PaymentResult result) {
        if (order.status().isFinal()) {
            log.warn("Order {} already in final state {}, payment result {} ignored", order.id(), order.status(),
                    result.eventId());
            return order;
        }
        var now = clock.instant();
        var next = switch (result) {
            case PaymentCompleted completed -> order.markPaid(completed.paymentId(), now);
            case PaymentFailed failed -> order.markPaymentFailed(failed.reason(), now);
        };
        log.info("Order {} status changed {} -> {}", order.id(), order.status(), next.status());
        return next;
    }
}
