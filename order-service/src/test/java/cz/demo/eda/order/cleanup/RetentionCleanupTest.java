package cz.demo.eda.order.cleanup;

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
    private static final Instant CUTOFF = Instant.parse("2026-09-30T03:00:00Z");

    @Mock
    private CleanupService service;

    private RetentionCleanup cleanup;

    @BeforeEach
    void setUp() {
        cleanup = new RetentionCleanup(service, new CleanupProperties(7, 100, "0 0 3 * * *"),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Smaže zprávy starší než retenční počet dní")
    void should_deleteOlderThanRetention_whenPurged() {
        when(service.deleteInboxBatch(CUTOFF, 100)).thenReturn(5);
        when(service.deleteOutboxBatch(CUTOFF, 100)).thenReturn(3);

        assertThat(cleanup.purge()).isEqualTo(8);
    }

    @Test
    @DisplayName("Plnou dávku opakuje, dokud se nesmaže méně než batchSize")
    void should_repeatBatches_whenBatchIsFull() {
        when(service.deleteInboxBatch(any(), anyInt())).thenReturn(100, 100, 7);
        when(service.deleteOutboxBatch(any(), anyInt())).thenReturn(0);

        assertThat(cleanup.purge()).isEqualTo(207);

        verify(service, times(3)).deleteInboxBatch(any(), anyInt());
        verify(service, times(1)).deleteOutboxBatch(any(), anyInt());
    }

    @Test
    @DisplayName("Chyba DB úklid ukončí bez vyhození výjimky")
    void should_returnZero_whenDeleteFails() {
        when(service.deleteInboxBatch(any(), anyInt())).thenThrow(new IllegalStateException("db down"));

        assertThat(cleanup.purge()).isZero();
    }
}
