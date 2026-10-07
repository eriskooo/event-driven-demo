package cz.demo.eda.payment.outbox;

import cz.demo.eda.payment.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ImportAutoConfiguration(JacksonAutoConfiguration.class)
@Import({PostgresTestcontainer.class, OutboxRepository.class})
class OutboxRepositoryTest {

    @Autowired
    private OutboxRepository repository;

    @Test
    @DisplayName("Vložená zpráva je nepublikovaná a vrátí se i s hlavičkami")
    void should_returnUnpublished_whenInserted() {
        var id = UUID.randomUUID();

        repository.insert(id, "orders.created", "o-1", "{\"orderId\": \"o-1\"}", Map.of("X-Correlation-Id", "c-1"));

        var batch = repository.lockUnpublished(10);
        assertThat(batch).singleElement().satisfies(m -> {
            assertThat(m.eventId()).isEqualTo(id);
            assertThat(m.topic()).isEqualTo("orders.created");
            assertThat(m.key()).isEqualTo("o-1");
            assertThat(m.payload()).contains("\"orderId\"");
            assertThat(m.headers()).containsEntry("X-Correlation-Id", "c-1");
        });
        assertThat(repository.countUnpublished()).isEqualTo(1);
    }

    @Test
    @DisplayName("Publikované zprávy už znovu nevrátí")
    void should_skipPublished_whenMarked() {
        repository.insert(UUID.randomUUID(), "t", "k", "{}", Map.of());
        var batch = repository.lockUnpublished(10);

        repository.markPublished(batch.stream().map(OutboxMessage::id).toList());

        assertThat(repository.lockUnpublished(10)).isEmpty();
        assertThat(repository.countUnpublished()).isZero();
    }

    @Test
    @DisplayName("Respektuje limit dávky a pořadí vložení")
    void should_returnOldestFirst_whenLimitApplied() {
        var first = UUID.randomUUID();
        var second = UUID.randomUUID();
        repository.insert(first, "t", "k", "{}", Map.of());
        repository.insert(second, "t", "k", "{}", Map.of());
        repository.insert(UUID.randomUUID(), "t", "k", "{}", Map.of());

        assertThat(repository.lockUnpublished(2)).extracting(OutboxMessage::eventId).containsExactly(first, second);
    }

    @Test
    @DisplayName("Úklid smaže jen publikované zprávy starší než cutoff, nepublikované nechá")
    void should_deleteOnlyPublishedOldMessages_whenCleanedUp() {
        repository.insert(UUID.randomUUID(), "t", "k", "{}", Map.of());
        repository.markPublished(repository.lockUnpublished(10).stream().map(OutboxMessage::id).toList());
        repository.insert(UUID.randomUUID(), "t", "k", "{}", Map.of());

        assertThat(repository.deletePublishedBefore(Instant.now().minusSeconds(3600), 100)).isZero();
        assertThat(repository.deletePublishedBefore(Instant.now().plusSeconds(60), 100)).isEqualTo(1);

        assertThat(repository.countUnpublished()).isEqualTo(1);
    }

    @Test
    @DisplayName("Prázdný seznam ID nic nezmění")
    void should_doNothing_whenMarkingEmptyList() {
        repository.insert(UUID.randomUUID(), "t", "k", "{}", Map.of());

        repository.markPublished(List.of());

        assertThat(repository.countUnpublished()).isEqualTo(1);
    }
}
