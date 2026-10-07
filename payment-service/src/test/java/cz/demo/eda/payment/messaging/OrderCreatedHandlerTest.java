package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.domain.PaymentService;
import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.inbox.InboxMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderCreatedHandlerTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Mock
    private PaymentService paymentService;

    @Test
    @DisplayName("Payload z inboxu deserializuje a předá ke zpracování platby")
    void should_processPayment_whenHandled() {
        var event = OrderCreated.of("c", "o-1", "cust", new BigDecimal("9.99"), "CZK");
        var handler = new OrderCreatedHandler(paymentService, jsonMapper);

        handler.handle(new InboxMessage(event.eventId(), "orders.created", "o-1", jsonMapper.writeValueAsString(event),
                "c", 0));

        verify(paymentService).processPayment(event);
    }

    @Test
    @DisplayName("Nečitelný payload vyhodí výjimku – inbox ji zpracuje jako neúspěšný pokus")
    void should_throw_whenPayloadInvalid() {
        var handler = new OrderCreatedHandler(paymentService, jsonMapper);

        assertThatThrownBy(() -> handler.handle(new InboxMessage(UUID.randomUUID(), "orders.created", "o-1",
                "not json", null, 0))).isInstanceOf(JacksonException.class);
        verifyNoInteractions(paymentService);
    }
}
