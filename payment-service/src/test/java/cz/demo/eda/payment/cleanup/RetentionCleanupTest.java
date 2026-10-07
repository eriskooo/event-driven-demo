package cz.demo.eda.payment.cleanup;

import cz.demo.eda.payment.inbox.InboxRepository;
import cz.demo.eda.payment.outbox.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetentionCleanupTest {

    private static final Instant NOW = Instant.parse("2026-10-07T03:00:00Z");

    @Mock
    private InboxRepository inbox;
    @Mock
    private OutboxRepository outbox;

    private SimpleMeterRegistry registry;
    private RetentionCleanup cleanup;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        cleanup = new RetentionCleanup(inbox, outbox, new CleanupProperties(7, 100, "0 0 3 * * *"), registry,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Smaže zprávy starší než retenční počet dní a započítá je")
    void should_deleteOlderThanRetention_whenPurged() {
        var cutoff = Instant.parse("2026-09-30T03:00:00Z");
        when(inbox.deleteFinishedBefore(cutoff, 100)).thenReturn(5);
        when(outbox.deletePublishedBefore(cutoff, 100)).thenReturn(3);

        assertThat(cleanup.purge()).isEqualTo(8);

        assertThat(registry.get(RetentionCleanup.DELETED_METRIC).tag("table", "inbox").counter().count()).isEqualTo(5.0);
        assertThat(registry.get(RetentionCleanup.DELETED_METRIC).tag("table", "outbox").counter().count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("Plnou dávku opakuje, dokud se nesmaže méně než batchSize")
    void should_repeatBatches_whenBatchIsFull() {
        when(inbox.deleteFinishedBefore(any(), anyInt())).thenReturn(100, 100, 7);
        when(outbox.deletePublishedBefore(any(), anyInt())).thenReturn(0);

        assertThat(cleanup.purge()).isEqualTo(207);

        verify(inbox, times(3)).deleteFinishedBefore(any(), anyInt());
        verify(outbox, times(1)).deletePublishedBefore(any(), anyInt());
    }

    @Test
    @DisplayName("Bez starých zpráv nic nesmaže")
    void should_returnZero_whenNothingToDelete() {
        when(inbox.deleteFinishedBefore(any(), anyInt())).thenReturn(0);
        when(outbox.deletePublishedBefore(any(), anyInt())).thenReturn(0);

        assertThat(cleanup.purge()).isZero();
    }

    @Test
    @DisplayName("Chyba DB úklid ukončí bez vyhození výjimky")
    void should_returnZero_whenRepositoryFails() {
        when(inbox.deleteFinishedBefore(any(), anyInt())).thenThrow(new IllegalStateException("db down"));

        assertThat(cleanup.purge()).isZero();
    }
}
