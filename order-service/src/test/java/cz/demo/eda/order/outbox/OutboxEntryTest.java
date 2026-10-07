package cz.demo.eda.order.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxEntryTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    @DisplayName("Nová zpráva je nepublikovaná a hlavičky jsou kopií vstupu")
    void should_bePendingWithCopiedHeaders_whenCreated() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("h", "v");

        OutboxEntry entry = OutboxEntry.pending(UUID.randomUUID(), "t", "k", "{}", headers, NOW);
        headers.put("later", "x");

        assertThat(entry.publishedAt()).isNull();
        assertThat(entry.createdAt()).isEqualTo(NOW);
        assertThat(entry.headers()).containsOnlyKeys("h");
    }

    @Test
    @DisplayName("Bez hlaviček vytvoří prázdnou mapu, kterou nejde zvenku měnit")
    void should_exposeImmutableEmptyHeaders_whenHeadersNull() {
        OutboxEntry entry = OutboxEntry.pending(UUID.randomUUID(), "t", "k", "{}", null, NOW);

        assertThat(entry.headers()).isEmpty();
        assertThatThrownBy(() -> entry.headers().put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Označení publikace uloží čas")
    void should_setPublishedAt_whenMarkedPublished() {
        OutboxEntry entry = OutboxEntry.pending(UUID.randomUUID(), "t", "k", "{}", null, NOW);

        entry.markPublished(NOW.plusSeconds(1));

        assertThat(entry.publishedAt()).isEqualTo(NOW.plusSeconds(1));
    }

    @Test
    @DisplayName("Odmítne zprávu bez klíče nebo payloadu")
    void should_throw_whenRequiredFieldMissing() {
        assertThatNullPointerException().isThrownBy(() -> OutboxEntry.pending(UUID.randomUUID(), "t", null, "{}", null, NOW));
        assertThatNullPointerException().isThrownBy(() -> OutboxEntry.pending(UUID.randomUUID(), "t", "k", null, null, NOW));
    }
}
