package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.CancelOrder;
import cz.demo.eda.order.event.ConfirmOrder;
import cz.demo.eda.order.inbox.InboxEntry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderCommandHandlerTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Mock
    private OrderService orderService;

    @Test
    @DisplayName("Payload z inboxu deserializuje na správný podtyp příkazu a předá doméně")
    void should_applyDeserializedCommand_whenHandled() {
        CancelOrder command = CancelOrder.of("c", "o-1", "declined");
        OrderCommandHandler handler = new OrderCommandHandler(orderService, jsonMapper);

        handler.handle(InboxEntry.received(command.eventId(), "orders.commands", "o-1",
                jsonMapper.writeValueAsString(command), "c", Instant.now()));

        verify(orderService).applyCommand(command);
    }

    @Test
    @DisplayName("Diskriminátor type=ConfirmOrder se deserializuje na ConfirmOrder a předá doméně")
    void should_applyConfirmOrder_whenTypeIsConfirmOrder() {
        ConfirmOrder command = ConfirmOrder.of("c", "o-1", "p-1");
        OrderCommandHandler handler = new OrderCommandHandler(orderService, jsonMapper);

        handler.handle(InboxEntry.received(command.eventId(), "orders.commands", "o-1",
                jsonMapper.writeValueAsString(command), "c", Instant.now()));

        verify(orderService).applyCommand(command);
    }

    @Test
    @DisplayName("Nečitelný payload vyhodí výjimku – inbox ji zpracuje jako neúspěšný pokus")
    void should_throw_whenPayloadInvalid() {
        OrderCommandHandler handler = new OrderCommandHandler(orderService, jsonMapper);

        assertThatThrownBy(() -> handler.handle(InboxEntry.received(UUID.randomUUID(), "orders.commands", "o-1",
                "{\"type\":\"Unknown\"}", null, Instant.now()))).isInstanceOf(JacksonException.class);
        verifyNoInteractions(orderService);
    }
}
