package cz.demo.eda.payment.outbox;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.inbox.InboxMessage;
import cz.demo.eda.payment.support.Tracing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxRepository repository;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(repository, JsonMapper.builder().build());
    }

    @Test
    @DisplayName("Událost uloží s klíčem orderId, JSON payloadem a hlavičkou correlationId")
    void should_storeEventAsJson_whenPublished() {
        var event = OrderCreated.of("corr-1", "o-1", "c-1", new BigDecimal("10.50"), "CZK");

        publisher.publish("orders.created", event);

        var payload = ArgumentCaptor.forClass(String.class);
        verify(repository).insert(eq(event.eventId()), eq("orders.created"), eq("o-1"), payload.capture(),
                eq(Map.of(Tracing.CORRELATION_ID_HEADER, "corr-1")));
        assertThat(payload.getValue()).contains("\"orderId\":\"o-1\"", "\"amount\":10.50", "\"eventId\"");
    }

    @Test
    @DisplayName("Bez correlationId uloží prázdné hlavičky")
    void should_storeNoHeaders_whenCorrelationIdIsNull() {
        var event = OrderCreated.of(null, "o-1", "c-1", BigDecimal.ONE, "CZK");

        publisher.publish("orders.created", event);

        verify(repository).insert(eq(event.eventId()), eq("orders.created"), eq("o-1"), anyString(), eq(Map.of()));
    }

    @Test
    @DisplayName("DLT zprávu pošle do <topic>.DLT s původním payloadem a důvodem selhání")
    void should_storeDeadLetterWithErrorHeaders_whenProcessingGaveUp() {
        var message = new InboxMessage(UUID.randomUUID(), "orders.created", "o-1", "{\"x\":1}", "corr-9", 3);

        publisher.publishDeadLetter(message, 4, new IllegalStateException("x".repeat(1500)));

        var headers = ArgumentCaptor.forClass(Map.class);
        verify(repository).insert(eq(message.eventId()), eq("orders.created.DLT"), eq("o-1"), eq("{\"x\":1}"),
                headers.capture());
        @SuppressWarnings("unchecked")
        Map<String, String> captured = headers.getValue();
        assertThat(captured)
                .containsEntry(Tracing.CORRELATION_ID_HEADER, "corr-9")
                .containsEntry(OutboxPublisher.DLT_ORIGINAL_TOPIC, "orders.created")
                .containsEntry(OutboxPublisher.DLT_EXCEPTION_FQCN, IllegalStateException.class.getName())
                .containsEntry(OutboxPublisher.DLT_ATTEMPTS, "4");
        assertThat(captured.get(OutboxPublisher.DLT_EXCEPTION_MESSAGE)).hasSize(1000);
    }
}
