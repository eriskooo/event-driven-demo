package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.domain.PaymentService;
import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.inbox.InboxEntry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class ProcessPaymentHandlerTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Mock
    private PaymentService paymentService;

    @Test
    @DisplayName("Payload z inboxu deserializuje a předá ke zpracování platby")
    void should_processPayment_whenHandled() {
        ProcessPayment event = ProcessPayment.of("c", "o-1", new BigDecimal("9.99"), "CZK");
        ProcessPaymentHandler handler = new ProcessPaymentHandler(paymentService, jsonMapper);

        handler.handle(InboxEntry.received(event.eventId(), "payments.commands", "o-1",
                jsonMapper.writeValueAsString(event), "c", Instant.now()));

        verify(paymentService).processPayment(event);
    }

    @Test
    @DisplayName("Nečitelný payload vyhodí výjimku – inbox ji zpracuje jako neúspěšný pokus")
    void should_throw_whenPayloadInvalid() {
        ProcessPaymentHandler handler = new ProcessPaymentHandler(paymentService, jsonMapper);

        assertThatThrownBy(() -> handler.handle(InboxEntry.received(UUID.randomUUID(), "payments.commands", "o-1",
                "not json", null, Instant.now()))).isInstanceOf(JacksonException.class);
        verifyNoInteractions(paymentService);
    }
}
