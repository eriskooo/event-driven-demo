package cz.demo.eda.payment.inbox;

import cz.demo.eda.payment.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresTestcontainer.class, InboxRepository.class})
class InboxRepositoryTest {

    private static final String PAYLOAD = "{\"orderId\":\"o-1\"}";

    @Autowired
    private InboxRepository repository;

    @Test
    @DisplayName("Novou zprávu uloží jako PENDING a vrátí ji ke zpracování")
    void should_storeAndReturnPending_whenNewMessage() {
        var id = UUID.randomUUID();

        assertThat(repository.store(id, "payments.result", "o-1", PAYLOAD, "corr-1")).isTrue();

        assertThat(repository.findStatus(id)).contains("PENDING");
        assertThat(repository.lockNextDue()).get().satisfies(m -> {
            assertThat(m.eventId()).isEqualTo(id);
            assertThat(m.key()).isEqualTo("o-1");
            assertThat(m.correlationId()).isEqualTo("corr-1");
            assertThat(m.attempts()).isZero();
            assertThat(m.payload()).contains("o-1");
        });
    }

    @Test
    @DisplayName("Duplicitní eventId podruhé neuloží")
    void should_returnFalse_whenEventIdAlreadyStored() {
        var id = UUID.randomUUID();
        repository.store(id, "t", "k", PAYLOAD, null);

        assertThat(repository.store(id, "t", "k", PAYLOAD, null)).isFalse();
    }

    @Test
    @DisplayName("Odmítne null eventId")
    void should_throw_whenEventIdIsNull() {
        assertThatNullPointerException().isThrownBy(() -> repository.store(null, "t", "k", PAYLOAD, null));
    }

    @Test
    @DisplayName("Prázdný inbox nevrátí nic ke zpracování")
    void should_returnEmpty_whenInboxEmpty() {
        assertThat(repository.lockNextDue()).isEmpty();
    }

    @Test
    @DisplayName("Zprávu s retry v budoucnu zatím nevrátí, s retry v minulosti ano")
    void should_respectNextAttempt_whenRetryScheduled() {
        var id = UUID.randomUUID();
        repository.store(id, "t", "k", PAYLOAD, null);

        repository.scheduleRetry(id, 1, Instant.now().plusSeconds(60), "boom");
        assertThat(repository.lockNextDue()).isEmpty();

        repository.scheduleRetry(id, 2, Instant.now().minusSeconds(1), "boom");
        assertThat(repository.lockNextDue()).get().extracting(InboxMessage::attempts).isEqualTo(2);
    }

    @Test
    @DisplayName("Zpracovanou ani neúspěšnou zprávu už nevrátí")
    void should_skipFinishedMessages_whenProcessedOrFailed() {
        var processed = UUID.randomUUID();
        var failed = UUID.randomUUID();
        repository.store(processed, "t", "k", PAYLOAD, null);
        repository.store(failed, "t", "k", PAYLOAD, null);

        repository.markProcessed(processed);
        repository.markFailed(failed, 4, "x".repeat(3000));

        assertThat(repository.findStatus(processed)).contains("PROCESSED");
        assertThat(repository.findStatus(failed)).contains("FAILED");
        assertThat(repository.lockNextDue()).isEmpty();
    }

    @Test
    @DisplayName("Úklid smaže jen dokončené zprávy starší než cutoff, čekající nechá")
    void should_deleteOnlyFinishedOldMessages_whenCleanedUp() {
        var processed = UUID.randomUUID();
        var failed = UUID.randomUUID();
        var pending = UUID.randomUUID();
        repository.store(processed, "t", "k", PAYLOAD, null);
        repository.store(failed, "t", "k", PAYLOAD, null);
        repository.store(pending, "t", "k", PAYLOAD, null);
        repository.markProcessed(processed);
        repository.markFailed(failed, 4, "boom");

        assertThat(repository.deleteFinishedBefore(Instant.now().minusSeconds(3600), 100)).isZero();
        assertThat(repository.deleteFinishedBefore(Instant.now().plusSeconds(60), 100)).isEqualTo(2);

        assertThat(repository.findStatus(processed)).isEmpty();
        assertThat(repository.findStatus(failed)).isEmpty();
        assertThat(repository.findStatus(pending)).contains("PENDING");
    }

    @Test
    @DisplayName("Úklid respektuje limit dávky")
    void should_respectLimit_whenCleaningUp() {
        for (int i = 0; i < 3; i++) {
            var id = UUID.randomUUID();
            repository.store(id, "t", "k", PAYLOAD, null);
            repository.markProcessed(id);
        }

        assertThat(repository.deleteFinishedBefore(Instant.now().plusSeconds(60), 2)).isEqualTo(2);
        assertThat(repository.deleteFinishedBefore(Instant.now().plusSeconds(60), 2)).isEqualTo(1);
    }

    @Test
    @DisplayName("Pro neznámé eventId vrátí prázdný stav")
    void should_returnEmptyStatus_whenUnknown() {
        assertThat(repository.findStatus(UUID.randomUUID())).isEmpty();
    }
}
