package cz.demo.eda.process.messaging;

import cz.demo.eda.process.event.PaymentCompleted;
import cz.demo.eda.process.event.PaymentFailed;
import cz.demo.eda.process.event.PaymentResult;
import cz.demo.eda.process.process.ProcessGateway;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Výsledek platby posune proces, který čeká na zprávu PaymentResult se stejným orderId. */
@Component
public class PaymentResultListener {

    static final String STATUS_COMPLETED = "COMPLETED";
    static final String STATUS_FAILED = "FAILED";

    private final ProcessGateway gateway;

    /** Vytvoří posluchač s bránou do procesu. */
    public PaymentResultListener(ProcessGateway gateway) {
        this.gateway = gateway;
    }

    /** Zkoreluje výsledek do procesu (correlationKey = orderId); duplicitu Zeebe odmítne podle eventId. */
    @KafkaListener(id = "payment-result-listener", idIsGroup = false, topics = Topics.PAYMENTS_RESULT,
            properties = "spring.json.value.default.type=cz.demo.eda.process.event.PaymentResult")
    public void onPaymentResult(PaymentResult result) {
        gateway.publish(ProcessMessages.PAYMENT_RESULT, result.orderId(), result.eventId(), variables(result));
    }

    private static Map<String, Object> variables(PaymentResult result) {
        Map<String, Object> variables = new HashMap<>();
        switch (result) {
            case PaymentCompleted completed -> {
                variables.put("paymentStatus", STATUS_COMPLETED);
                variables.put("paymentId", completed.paymentId());
                variables.put("failureReason", null);
            }
            case PaymentFailed failed -> {
                variables.put("paymentStatus", STATUS_FAILED);
                variables.put("paymentId", null);
                variables.put("failureReason", failed.reason());
            }
        }
        return Collections.unmodifiableMap(variables);
    }
}
