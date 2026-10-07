package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.PaymentResult;
import cz.demo.eda.order.inbox.InboxMessage;
import cz.demo.eda.order.inbox.InboxMessageHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Zpracuje výsledek platby uložený v inboxu. */
@Component
public class PaymentResultHandler implements InboxMessageHandler {

    private final OrderService orderService;
    private final JsonMapper jsonMapper;

    public PaymentResultHandler(OrderService orderService, JsonMapper jsonMapper) {
        this.orderService = orderService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void handle(InboxMessage message) {
        orderService.applyPaymentResult(jsonMapper.readValue(message.payload(), PaymentResult.class));
    }
}
