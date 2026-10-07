package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.PaymentFailed;
import cz.demo.eda.order.inbox.InboxMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class PaymentResultHandlerTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Mock
    private OrderService orderService;

    @Test
    @DisplayName("Payload z inboxu deserializuje na správný podtyp a předá doméně")
    void should_applyDeserializedResult_whenHandled() {
        var event = PaymentFailed.of("c", "o-1", "declined");
        var handler = new PaymentResultHandler(orderService, jsonMapper);

        handler.handle(new InboxMessage(event.eventId(), "payments.result", "o-1",
                jsonMapper.writeValueAsString(event), "c", 0));

        verify(orderService).applyPaymentResult(event);
    }

    @Test
    @DisplayName("Nečitelný payload vyhodí výjimku – inbox ji zpracuje jako neúspěšný pokus")
    void should_throw_whenPayloadInvalid() {
        var handler = new PaymentResultHandler(orderService, jsonMapper);

        assertThatThrownBy(() -> handler.handle(new InboxMessage(UUID.randomUUID(), "payments.result", "o-1",
                "{\"type\":\"Unknown\"}", null, 0))).isInstanceOf(JacksonException.class);
        verifyNoInteractions(orderService);
    }
}
