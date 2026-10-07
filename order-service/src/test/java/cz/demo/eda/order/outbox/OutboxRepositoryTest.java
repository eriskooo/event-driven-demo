package cz.demo.eda.order.outbox;

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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(PostgresTestcontainer.class)
class OutboxRepositoryTest {

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private OutboxRepository repository;
    @Autowired
    private TestEntityManager em;

    @Test
    @DisplayName("Uložená zpráva se načte z DB i s JSON payloadem a hlavičkami")
    void should_roundTripPayloadAndHeaders_whenSaved() {
        UUID eventId = UUID.randomUUID();
        repository.save(entry(eventId, "{\"orderId\": \"o-1\"}", Map.of("X-Correlation-Id", "c-1")));
        em.flush();
        em.clear();

        assertThat(repository.lockUnpublished(Limit.of(10))).singleElement().satisfies(e -> {
            assertThat(e.id()).isNotNull();
            assertThat(e.eventId()).isEqualTo(eventId);
            assertThat(e.messageKey()).isEqualTo("o-1");
            assertThat(e.payload()).contains("\"orderId\"", "o-1");
            assertThat(e.headers()).containsExactlyEntriesOf(Map.of("X-Correlation-Id", "c-1"));
            assertThat(e.createdAt()).isEqualTo(NOW);
        });
        assertThat(repository.countByPublishedAtIsNull()).isEqualTo(1);
    }

    @Test
    @DisplayName("Publikované zprávy už znovu nevrátí")
    void should_skipPublished_whenMarked() {
        repository.save(entry(UUID.randomUUID(), "{}", Map.of()));
        em.flush();

        repository.lockUnpublished(Limit.of(10)).forEach(e -> e.markPublished(NOW));
        em.flush();

        assertThat(repository.lockUnpublished(Limit.of(10))).isEmpty();
        assertThat(repository.countByPublishedAtIsNull()).isZero();
    }

    @Test
    @DisplayName("Respektuje limit dávky a pořadí vložení")
    void should_returnOldestFirst_whenLimitApplied() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        repository.save(entry(first, "{}", Map.of()));
        repository.save(entry(second, "{}", Map.of()));
        repository.save(entry(UUID.randomUUID(), "{}", Map.of()));
        em.flush();

        assertThat(repository.lockUnpublished(Limit.of(2))).extracting(OutboxEntry::eventId).containsExactly(first, second);
    }

    @Test
    @DisplayName("Pro úklid vrátí jen publikované zprávy starší než cutoff")
    void should_returnOnlyPublishedOldIds_whenCleaningUp() {
        OutboxEntry published = entry(UUID.randomUUID(), "{}", Map.of());
        published.markPublished(NOW.minusSeconds(3600));
        repository.save(published);
        repository.save(entry(UUID.randomUUID(), "{}", Map.of()));
        em.flush();

        assertThat(repository.findPublishedIdsBefore(NOW.minusSeconds(7200), Limit.of(10))).isEmpty();
        assertThat(repository.findPublishedIdsBefore(NOW, Limit.of(10))).containsExactly(published.id());
    }

    private static OutboxEntry entry(UUID eventId, String payload, Map<String, String> headers) {
        return OutboxEntry.pending(eventId, "orders.created", "o-1", payload, headers, NOW);
    }
}
