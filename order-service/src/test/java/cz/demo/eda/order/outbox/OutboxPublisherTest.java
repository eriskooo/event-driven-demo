package cz.demo.eda.order.outbox;

import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.support.Tracing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private OutboxRepository repository;

    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(repository, JsonMapper.builder().build(), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Událost uloží s klíčem orderId, JSON payloadem a hlavičkou correlationId")
    void should_storeEventAsJson_whenPublished() {
        OrderCreated event = OrderCreated.of("corr-1", "o-1", "c-1", new BigDecimal("10.50"), "CZK");

        publisher.publish("orders.created", event);

        OutboxEntry saved = captureSaved();
        assertThat(saved.eventId()).isEqualTo(event.eventId());
        assertThat(saved.topic()).isEqualTo("orders.created");
        assertThat(saved.messageKey()).isEqualTo("o-1");
        assertThat(saved.payload()).contains("\"orderId\":\"o-1\"", "\"amount\":10.50", "\"eventId\"");
        assertThat(saved.headers()).containsExactlyEntriesOf(Map.of(Tracing.CORRELATION_ID_HEADER, "corr-1"));
        assertThat(saved.createdAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("Bez correlationId uloží prázdné hlavičky")
    void should_storeNoHeaders_whenCorrelationIdIsNull() {
        publisher.publish("orders.created", OrderCreated.of(null, "o-1", "c-1", BigDecimal.ONE, "CZK"));

        assertThat(captureSaved().headers()).isEmpty();
    }

    @Test
    @DisplayName("DLT zprávu pošle do <topic>.DLT s původním payloadem, počtem pokusů a důvodem selhání")
    void should_storeDeadLetterWithErrorHeaders_whenProcessingGaveUp() {
        InboxEntry entry = InboxEntry.received(UUID.randomUUID(), "payments.result", "o-1", "{\"x\":1}", "corr-9", NOW);
        entry.markFailed("boom", NOW);

        publisher.publishDeadLetter(entry, new IllegalStateException("x".repeat(1500)));

        OutboxEntry saved = captureSaved();
        assertThat(saved.topic()).isEqualTo("payments.result.DLT");
        assertThat(saved.eventId()).isEqualTo(entry.eventId());
        assertThat(saved.messageKey()).isEqualTo("o-1");
        assertThat(saved.payload()).isEqualTo("{\"x\":1}");
        assertThat(saved.headers())
                .containsEntry(Tracing.CORRELATION_ID_HEADER, "corr-9")
                .containsEntry(OutboxPublisher.DLT_ORIGINAL_TOPIC, "payments.result")
                .containsEntry(OutboxPublisher.DLT_EXCEPTION_FQCN, IllegalStateException.class.getName())
                .containsEntry(OutboxPublisher.DLT_ATTEMPTS, "1");
        assertThat(saved.headers().get(OutboxPublisher.DLT_EXCEPTION_MESSAGE)).hasSize(1000);
    }

    private OutboxEntry captureSaved() {
        ArgumentCaptor<OutboxEntry> captor = ArgumentCaptor.forClass(OutboxEntry.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }
}
