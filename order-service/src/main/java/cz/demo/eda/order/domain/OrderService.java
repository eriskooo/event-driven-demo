package cz.demo.eda.order.domain;

import cz.demo.eda.order.event.CancelOrder;
import cz.demo.eda.order.event.ConfirmOrder;
import cz.demo.eda.order.event.OrderCommand;
import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/** Doménová logika objednávek: založení a provedení příkazů orchestrátoru. */
@Service
@Transactional
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
    public Order createOrder(String customerId, BigDecimal amount, String currency, String correlationId) {
        Order order = repository.save(Order.create(UUID.randomUUID().toString(), customerId, amount, currency, clock.instant()));
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
     * Provede příkaz orchestrátoru nad objednávkou; vrací prázdno pro neznámou objednávku.
     * Volá ho InboxService ve své transakci (deduplikaci už zajistil inbox).
     */
    public Optional<Order> applyCommand(OrderCommand command) {
        Optional<Order> order = repository.findForUpdate(command.orderId());
        if (order.isEmpty()) {
            // Příkaz pro neznámou objednávku retry nespraví – jen varování, zpráva se označí jako zpracovaná.
            log.warn("Command {} for unknown order {} ignored", command.eventId(), command.orderId());
            return order;
        }
        // Změna se uloží dirty checkingem při commitu transakce.
        transition(order.get(), command);
        return order;
    }

    private void transition(Order order, OrderCommand command) {
        if (order.status().isFinal()) {
            log.warn("Order {} already in final state {}, command {} ignored", order.id(), order.status(),
                    command.eventId());
            return;
        }
        OrderStatus previous = order.status();
        Instant now = clock.instant();
        switch (command) {
            case ConfirmOrder confirm -> order.markPaid(confirm.paymentId(), now);
            case CancelOrder cancel -> order.markPaymentFailed(cancel.reason(), now);
        }
        log.info("Order {} status changed {} -> {}", order.id(), previous, order.status());
    }
}
