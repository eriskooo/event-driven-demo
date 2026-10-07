package cz.demo.eda.order.inbox;

import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.MessagingMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InboxProcessorTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private InboxRepository repository;
    @Mock
    private InboxMessageHandler handler;
    @Mock
    private OutboxPublisher outbox;

    private SimpleMeterRegistry registry;
    private InboxProcessor processor;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        var properties = new InboxProperties(4, Duration.ofMillis(500), 2.0, Duration.ofSeconds(5), 3);
        processor = new InboxProcessor(repository, handler, outbox, properties, new MessagingMetrics(registry),
                TransactionOperations.withoutTransaction(), TransactionOperations.withoutTransaction(),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Úspěšně zpracovanou zprávu označí jako PROCESSED")
    void should_markProcessed_whenHandlerSucceeds() {
        var message = message(0);
        when(repository.lockNextDue()).thenReturn(Optional.of(message));

        assertThat(processor.processNext()).isTrue();

        verify(handler).handle(message);
        verify(repository).markProcessed(message.eventId());
        assertThat(inboxCount(MessagingMetrics.INBOX_SUCCESS)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez čekajících zpráv nic nedělá a vrátí false")
    void should_returnFalse_whenNothingDue() {
        when(repository.lockNextDue()).thenReturn(Optional.empty());

        assertThat(processor.processNext()).isFalse();

        verifyNoInteractions(handler, outbox);
    }

    @Test
    @DisplayName("Při chybě naplánuje retry s backoffem podle počtu pokusů")
    void should_scheduleRetry_whenHandlerFailsBeforeLimit() {
        var message = message(1);
        when(repository.lockNextDue()).thenReturn(Optional.of(message));
        doThrow(new IllegalStateException("boom")).when(handler).handle(message);

        processor.processNext();

        // Druhý neúspěšný pokus → backoff 1 s.
        verify(repository).scheduleRetry(eq(message.eventId()), eq(2), eq(NOW.plusSeconds(1)), contains("boom"));
        verify(repository, never()).markProcessed(any());
        verifyNoInteractions(outbox);
        assertThat(inboxCount(MessagingMetrics.INBOX_RETRY)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Po vyčerpání pokusů označí zprávu FAILED a pošle ji do DLT")
    void should_deadLetter_whenAttemptsExhausted() {
        var message = message(3);
        var error = new IllegalStateException("boom");
        when(repository.lockNextDue()).thenReturn(Optional.of(message));
        doThrow(error).when(handler).handle(message);

        processor.processNext();

        verify(repository).markFailed(eq(message.eventId()), eq(4), contains("boom"));
        verify(outbox).publishDeadLetter(message, 4, error);
        verify(repository, never()).scheduleRetry(any(), anyInt(), any(), any());
        assertThat(registry.get(MessagingMetrics.DEAD_LETTERED).tag("topic", "payments.result.DLT").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("Jeden běh zpracuje nejvýše batchSize zpráv")
    void should_stopAtBatchSize_whenMoreMessagesDue() {
        when(repository.lockNextDue()).thenAnswer(inv -> Optional.of(message(0)));

        processor.processDue();

        verify(handler, times(3)).handle(any());
    }

    @Test
    @DisplayName("Chyba infrastruktury běh ukončí bez vyhození výjimky")
    void should_swallowAndLog_whenRepositoryFails() {
        when(repository.lockNextDue()).thenThrow(new IllegalStateException("db down"));

        processor.processDue();

        verifyNoInteractions(handler);
    }

    private double inboxCount(String outcome) {
        var counter = registry.find(MessagingMetrics.INBOX_PROCESSED).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static InboxMessage message(int attempts) {
        return new InboxMessage(UUID.randomUUID(), "payments.result", "o-1", "{}", "corr-1", attempts);
    }
}
