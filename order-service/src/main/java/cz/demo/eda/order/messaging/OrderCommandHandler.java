package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.OrderCommand;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.inbox.InboxMessageHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Zpracuje příkaz orchestrátoru uložený v inboxu. */
@Component
public class OrderCommandHandler implements InboxMessageHandler {

    private final OrderService orderService;
    private final JsonMapper jsonMapper;

    public OrderCommandHandler(OrderService orderService, JsonMapper jsonMapper) {
        this.orderService = orderService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void handle(InboxEntry entry) {
        orderService.applyCommand(jsonMapper.readValue(entry.payload(), OrderCommand.class));
    }
}
