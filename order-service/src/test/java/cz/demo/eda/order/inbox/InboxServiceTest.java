package cz.demo.eda.order.inbox;

import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.MessagingMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InboxServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private InboxRepository repository;
    @Mock
    private InboxMessageHandler handler;
    @Mock
    private OutboxPublisher outbox;

    private SimpleMeterRegistry registry;
    private InboxService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        InboxProperties properties = new InboxProperties(4, Duration.ofMillis(500), 2.0, Duration.ofSeconds(5), 3);
        service = new InboxService(repository, handler, outbox, properties, new MessagingMetrics(registry),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Novou zprávu uloží a vrátí true")
    void should_saveAndReturnTrue_whenNewMessage() {
        InboxEntry entry = entry(0);
        when(repository.existsById(entry.eventId())).thenReturn(false);

        assertThat(service.store(entry)).isTrue();

        verify(repository).save(entry);
    }

    @Test
    @DisplayName("Duplicitní zprávu neuloží a vrátí false")
    void should_returnFalse_whenAlreadyStored() {
        InboxEntry entry = entry(0);
        when(repository.existsById(entry.eventId())).thenReturn(true);

        assertThat(service.store(entry)).isFalse();

        verify(repository, never()).save(any());
    }

    @Test
    @DisplayName("Vrátí ID zpráv k zpracování s limitem dávky")
    void should_returnDueIds_withBatchLimit() {
        UUID id = UUID.randomUUID();
        when(repository.findDueIds(NOW, Limit.of(3))).thenReturn(List.of(id));

        assertThat(service.findDueIds()).containsExactly(id);
    }

    @Test
    @DisplayName("Úspěšně zpracovanou zprávu označí jako PROCESSED")
    void should_markProcessed_whenHandlerSucceeds() {
        InboxEntry entry = entry(0);
        when(repository.lockIfDue(entry.eventId(), NOW)).thenReturn(Optional.of(entry));

        assertThat(service.process(entry.eventId())).isTrue();

        verify(handler).handle(entry);
        assertThat(entry.status()).isEqualTo(InboxStatus.PROCESSED);
        assertThat(entry.processedAt()).isEqualTo(NOW);
        assertThat(inboxCount(MessagingMetrics.INBOX_SUCCESS)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Zprávu, která už nečeká nebo je zamčená jinde, přeskočí")
    void should_returnFalse_whenNotLockable() {
        UUID id = UUID.randomUUID();
        when(repository.lockIfDue(id, NOW)).thenReturn(Optional.empty());

        assertThat(service.process(id)).isFalse();

        verifyNoInteractions(handler);
    }

    @Test
    @DisplayName("Chyba handleru se propaguje (rollback) a zpráva zůstane PENDING")
    void should_propagateAndKeepPending_whenHandlerFails() {
        InboxEntry entry = entry(0);
        when(repository.lockIfDue(entry.eventId(), NOW)).thenReturn(Optional.of(entry));
        doThrow(new IllegalStateException("boom")).when(handler).handle(entry);

        assertThatIllegalStateException().isThrownBy(() -> service.process(entry.eventId()));

        assertThat(entry.status()).isEqualTo(InboxStatus.PENDING);
    }

    @Test
    @DisplayName("Neúspěšný pokus naplánuje retry s backoffem podle počtu pokusů")
    void should_scheduleRetry_whenFailureBeforeLimit() {
        InboxEntry entry = entry(1);
        when(repository.findForUpdate(entry.eventId())).thenReturn(Optional.of(entry));

        service.recordFailure(entry.eventId(), new IllegalStateException("boom"));

        // Druhý neúspěšný pokus → backoff 1 s.
        assertThat(entry.attempts()).isEqualTo(2);
        assertThat(entry.nextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
        assertThat(entry.lastError()).contains("boom");
        assertThat(entry.status()).isEqualTo(InboxStatus.PENDING);
        verifyNoInteractions(outbox);
        assertThat(inboxCount(MessagingMetrics.INBOX_RETRY)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Po vyčerpání pokusů označí zprávu FAILED a pošle ji do DLT")
    void should_deadLetter_whenAttemptsExhausted() {
        InboxEntry entry = entry(3);
        IllegalStateException error = new IllegalStateException("boom");
        when(repository.findForUpdate(entry.eventId())).thenReturn(Optional.of(entry));

        service.recordFailure(entry.eventId(), error);

        assertThat(entry.status()).isEqualTo(InboxStatus.FAILED);
        assertThat(entry.attempts()).isEqualTo(4);
        verify(outbox).publishDeadLetter(entry, error);
        assertThat(registry.get(MessagingMetrics.DEAD_LETTERED).tag("topic", "payments.result.DLT").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("Selhání u zprávy, která už nečeká, nic nezmění")
    void should_ignoreFailure_whenEntryNoLongerPending() {
        InboxEntry entry = entry(0);
        entry.markProcessed(NOW);
        when(repository.findForUpdate(entry.eventId())).thenReturn(Optional.of(entry));

        service.recordFailure(entry.eventId(), new IllegalStateException("late"));

        assertThat(entry.status()).isEqualTo(InboxStatus.PROCESSED);
        assertThat(entry.attempts()).isZero();
        verifyNoInteractions(outbox);
    }

    private double inboxCount(String outcome) {
        Counter counter = registry.find(MessagingMetrics.INBOX_PROCESSED).tag("outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }

    private static InboxEntry entry(int failedAttempts) {
        InboxEntry entry = InboxEntry.received(UUID.randomUUID(), "payments.result", "o-1", "{}", "corr-1", NOW.minusSeconds(10));
        for (int i = 0; i < failedAttempts; i++) {
            entry.scheduleRetry(NOW.minusSeconds(1), "previous");
        }
        return entry;
    }
}
