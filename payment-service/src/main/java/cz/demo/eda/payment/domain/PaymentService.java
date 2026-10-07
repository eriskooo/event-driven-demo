package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentResult;
import cz.demo.eda.payment.outbox.OutboxPublisher;
import cz.demo.eda.payment.support.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

/** Zpracování platby za vytvořenou objednávku. */
@Service
@Transactional
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentSimulator simulator;
    private final PaymentRepository repository;
    private final OutboxPublisher outbox;
    private final Clock clock;

    public PaymentService(PaymentSimulator simulator, PaymentRepository repository, OutboxPublisher outbox, Clock clock) {
        this.simulator = simulator;
        this.repository = repository;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * Provede platbu, uloží ji a zařadí výsledek do outboxu – vše v jedné transakci
     * (volá ji InboxService, deduplikaci už zajistil inbox).
     *
     * @throws PaymentProcessingException technická chyba (poison částka) – inbox ji zopakuje, pak DLT
     */
    public PaymentResult processPayment(OrderCreated order) {
        log.info("Processing payment for order {} amount {} {}", order.orderId(), order.amount(), order.currency());
        PaymentResult result = simulator.process(order);
        repository.save(Payment.of(order, result, clock.instant()));
        outbox.publish(Topics.PAYMENTS_RESULT, result);
        log.info("Payment for order {} finished with {}", order.orderId(), result.getClass().getSimpleName());
        return result;
    }
}
