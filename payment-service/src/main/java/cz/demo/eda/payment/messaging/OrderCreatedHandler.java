package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.domain.PaymentService;
import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.inbox.InboxMessage;
import cz.demo.eda.payment.inbox.InboxMessageHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Zpracuje vytvořenou objednávku uloženou v inboxu. */
@Component
public class OrderCreatedHandler implements InboxMessageHandler {

    private final PaymentService paymentService;
    private final JsonMapper jsonMapper;

    public OrderCreatedHandler(PaymentService paymentService, JsonMapper jsonMapper) {
        this.paymentService = paymentService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void handle(InboxMessage message) {
        paymentService.processPayment(jsonMapper.readValue(message.payload(), OrderCreated.class));
    }
}
