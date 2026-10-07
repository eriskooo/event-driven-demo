package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.event.PaymentResult;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.random.RandomGenerator;

/** Simuluje platební bránu: náhodné zamítnutí podle failure-rate a "otrávená" částka pro demo DLT. */
@Component
public class PaymentSimulator {

    static final String DECLINED_REASON = "Payment declined by simulated gateway";

    private final PaymentProperties properties;
    private final RandomGenerator random;

    public PaymentSimulator(PaymentProperties properties, RandomGenerator random) {
        this.properties = properties;
        this.random = random;
    }

    /**
     * Zpracuje platbu za objednávku.
     *
     * @throws PaymentProcessingException pokud částka odpovídá nastavené poison částce
     */
    public PaymentResult process(OrderCreated order) {
        if (order.amount().compareTo(properties.poisonAmount()) == 0) {
            throw new PaymentProcessingException("Simulated gateway crash for poison amount " + order.amount()
                    + " (order " + order.orderId() + ")");
        }
        if (random.nextDouble() < properties.failureRate()) {
            return PaymentFailed.of(order.correlationId(), order.orderId(), DECLINED_REASON);
        }
        return PaymentCompleted.of(order.correlationId(), order.orderId(), UUID.randomUUID().toString(), order.amount());
    }
}
