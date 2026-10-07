package cz.demo.eda.payment.inbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class InboxEntryTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    @DisplayName("Přijatá zpráva čeká na okamžité zpracování bez pokusů")
    void should_bePendingAndDueNow_whenReceived() {
        InboxEntry entry = entry();

        assertThat(entry.status()).isEqualTo(InboxStatus.PENDING);
        assertThat(entry.attempts()).isZero();
        assertThat(entry.nextAttemptAt()).isEqualTo(NOW);
        assertThat(entry.receivedAt()).isEqualTo(NOW);
        assertThat(entry.processedAt()).isNull();
        assertThat(entry.isNew()).isTrue();
    }

    @Test
    @DisplayName("Retry zvýší počet pokusů, posune čas a uloží chybu")
    void should_incrementAttempts_whenRetryScheduled() {
        InboxEntry entry = entry();

        entry.scheduleRetry(NOW.plusSeconds(1), "boom");

        assertThat(entry.status()).isEqualTo(InboxStatus.PENDING);
        assertThat(entry.attempts()).isEqualTo(1);
        assertThat(entry.nextAttemptAt()).isEqualTo(NOW.plusSeconds(1));
        assertThat(entry.lastError()).isEqualTo("boom");
    }

    @Test
    @DisplayName("Úspěšné zpracování nastaví PROCESSED a smaže poslední chybu")
    void should_beProcessed_whenMarkedProcessed() {
        InboxEntry entry = entry();
        entry.scheduleRetry(NOW, "boom");

        entry.markProcessed(NOW.plusSeconds(2));

        assertThat(entry.status()).isEqualTo(InboxStatus.PROCESSED);
        assertThat(entry.processedAt()).isEqualTo(NOW.plusSeconds(2));
        assertThat(entry.lastError()).isNull();
    }

    @Test
    @DisplayName("Vzdání se nastaví FAILED, započítá pokus a ořízne dlouhou chybu")
    void should_beFailedWithTruncatedError_whenMarkedFailed() {
        InboxEntry entry = entry();

        entry.markFailed("x".repeat(5000), NOW);

        assertThat(entry.status()).isEqualTo(InboxStatus.FAILED);
        assertThat(entry.attempts()).isEqualTo(1);
        assertThat(entry.lastError()).hasSize(InboxEntry.MAX_ERROR_LENGTH);
        assertThat(entry.processedAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("Odmítne zprávu bez eventId nebo payloadu")
    void should_throw_whenRequiredFieldMissing() {
        assertThatNullPointerException().isThrownBy(() -> InboxEntry.received(null, "t", "k", "{}", null, NOW));
        assertThatNullPointerException().isThrownBy(() -> InboxEntry.received(UUID.randomUUID(), "t", "k", null, null, NOW));
    }

    private static InboxEntry entry() {
        return InboxEntry.received(UUID.randomUUID(), "orders.created", "o-1", "{}", "corr", NOW);
    }
}
