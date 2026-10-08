package cz.demo.eda.process.messaging;

import cz.demo.eda.process.event.OrderCreated;
import cz.demo.eda.process.process.ProcessGateway;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Založená objednávka spustí instanci procesu order-fulfillment (message start event). */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ProcessGateway gateway;

    /** Vytvoří posluchač s bránou do procesu. */
    public OrderCreatedListener(ProcessGateway gateway) {
        this.gateway = gateway;
    }

    /** Publikuje zprávu OrderCreated; duplicitní eventId Zeebe odmítne, takže druhá instance nevznikne. */
    @KafkaListener(id = "order-created-listener", idIsGroup = false, topics = Topics.ORDERS_CREATED,
            properties = "spring.json.value.default.type=cz.demo.eda.process.event.OrderCreated")
    public void onOrderCreated(OrderCreated event) {
        boolean started = gateway.publish(ProcessMessages.ORDER_CREATED, event.orderId(), event.eventId(), variables(event));
        if (started) {
            log.info("Process {} requested for order {}", ProcessMessages.PROCESS_ID, event.orderId());
        }
    }

    private static Map<String, Object> variables(OrderCreated event) {
        // HashMap kvůli null hodnotám (correlationId nemusí být vyplněné) – Map.of je nepřipouští.
        Map<String, Object> variables = new HashMap<>();
        variables.put("orderId", event.orderId());
        variables.put("amount", event.amount());
        variables.put("currency", event.currency());
        variables.put("correlationId", event.correlationId());
        return Collections.unmodifiableMap(variables);
    }
}
