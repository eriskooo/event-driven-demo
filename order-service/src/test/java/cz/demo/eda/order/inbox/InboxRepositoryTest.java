package cz.demo.eda.order.inbox;

import cz.demo.eda.order.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Limit;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(PostgresTestcontainer.class)
class InboxRepositoryTest {

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);
    private static final String PAYLOAD = "{\"orderId\": \"o-1\", \"type\": \"ConfirmOrder\"}";

    @Autowired
    private InboxRepository repository;
    @Autowired
    private TestEntityManager em;

    @Test
    @DisplayName("Uloženou zprávu načte z DB i s JSON payloadem")
    void should_loadAllFields_whenSaved() {
        UUID id = UUID.randomUUID();
        repository.save(entry(id));
        em.flush();
        em.clear();

        assertThat(repository.findById(id)).get().satisfies(e -> {
            assertThat(e.status()).isEqualTo(InboxStatus.PENDING);
            assertThat(e.messageKey()).isEqualTo("o-1");
            assertThat(e.correlationId()).isEqualTo("corr-1");
            assertThat(e.payload()).contains("\"orderId\"", "o-1");
            assertThat(e.receivedAt()).isEqualTo(NOW);
            assertThat(e.isNew()).isFalse();
        });
        assertThat(repository.existsById(id)).isTrue();
    }

    @Test
    @DisplayName("Mezi připravenými jsou jen čekající zprávy, jejichž čas nastal, od nejstarší")
    void should_returnOnlyDuePendingIds_whenQueried() {
        InboxEntry due = entry(UUID.randomUUID());
        InboxEntry later = entry(UUID.randomUUID());
        later.scheduleRetry(NOW.plusSeconds(60), "boom");
        InboxEntry done = entry(UUID.randomUUID());
        done.markProcessed(NOW);
        repository.save(due);
        repository.save(later);
        repository.save(done);
        em.flush();

        assertThat(repository.findDueIds(NOW.plusSeconds(1), Limit.of(10))).containsExactly(due.eventId());
        assertThat(repository.findDueIds(NOW.minusSeconds(1), Limit.of(10))).isEmpty();
    }

    @Test
    @DisplayName("Zamkne jen zprávu, která čeká a jejíž čas nastal")
    void should_lockOnlyDuePendingEntry_whenLocking() {
        InboxEntry entry = entry(UUID.randomUUID());
        repository.save(entry);
        em.flush();
        em.clear();

        assertThat(repository.lockIfDue(entry.eventId(), NOW.minusSeconds(1))).isEmpty();
        assertThat(repository.lockIfDue(entry.eventId(), NOW.plusSeconds(1))).isPresent();
        assertThat(repository.lockIfDue(UUID.randomUUID(), NOW.plusSeconds(1))).isEmpty();
    }

    @Test
    @DisplayName("Stav a pokusy se uloží dirty checkingem")
    void should_persistFailedState_whenEntryModified() {
        UUID id = UUID.randomUUID();
        repository.save(entry(id));
        em.flush();
        em.clear();

        repository.findForUpdate(id).orElseThrow().markFailed("boom", NOW);
        em.flush();
        em.clear();

        assertThat(repository.findById(id)).get().satisfies(e -> {
            assertThat(e.status()).isEqualTo(InboxStatus.FAILED);
            assertThat(e.attempts()).isEqualTo(1);
            assertThat(e.lastError()).isEqualTo("boom");
        });
    }

    @Test
    @DisplayName("Pro úklid vrátí jen dokončené zprávy starší než cutoff, s limitem")
    void should_returnOnlyFinishedOldIds_whenCleaningUp() {
        InboxEntry processed = entry(UUID.randomUUID());
        processed.markProcessed(NOW.minusSeconds(3600));
        InboxEntry failed = entry(UUID.randomUUID());
        failed.markFailed("boom", NOW.minusSeconds(3600));
        InboxEntry pending = entry(UUID.randomUUID());
        repository.save(processed);
        repository.save(failed);
        repository.save(pending);
        em.flush();

        assertThat(repository.findFinishedIdsBefore(NOW.minusSeconds(7200), Limit.of(10))).isEmpty();
        assertThat(repository.findFinishedIdsBefore(NOW, Limit.of(10)))
                .containsExactlyInAnyOrder(processed.eventId(), failed.eventId());
        assertThat(repository.findFinishedIdsBefore(NOW, Limit.of(1))).hasSize(1);
    }

    private static InboxEntry entry(UUID id) {
        return InboxEntry.received(id, "payments.result", "o-1", PAYLOAD, "corr-1", NOW);
    }
}
