package cz.demo.eda.order.cleanup;

import cz.demo.eda.order.inbox.InboxRepository;
import cz.demo.eda.order.outbox.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CleanupServiceTest {

    private static final Instant CUTOFF = Instant.parse("2026-09-30T03:00:00Z");

    @Mock
    private InboxRepository inbox;
    @Mock
    private OutboxRepository outbox;

    private SimpleMeterRegistry registry;
    private CleanupService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new CleanupService(inbox, outbox, registry);
    }

    @Test
    @DisplayName("Smaže nalezené dokončené zprávy inboxu a započítá je")
    void should_deleteFinishedInboxMessages_whenFound() {
        List<UUID> ids = List.of(UUID.randomUUID(), UUID.randomUUID());
        when(inbox.findFinishedIdsBefore(CUTOFF, Limit.of(100))).thenReturn(ids);

        assertThat(service.deleteInboxBatch(CUTOFF, 100)).isEqualTo(2);

        verify(inbox).deleteAllByIdInBatch(ids);
        assertThat(registry.get(CleanupService.DELETED_METRIC).tag("table", "inbox").counter().count()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("Smaže nalezené publikované zprávy outboxu a započítá je")
    void should_deletePublishedOutboxMessages_whenFound() {
        List<Long> ids = List.of(1L, 2L, 3L);
        when(outbox.findPublishedIdsBefore(CUTOFF, Limit.of(10))).thenReturn(ids);

        assertThat(service.deleteOutboxBatch(CUTOFF, 10)).isEqualTo(3);

        verify(outbox).deleteAllByIdInBatch(ids);
        assertThat(registry.get(CleanupService.DELETED_METRIC).tag("table", "outbox").counter().count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("Bez starých zpráv nic nesmaže")
    void should_returnZero_whenNothingFound() {
        when(inbox.findFinishedIdsBefore(CUTOFF, Limit.of(100))).thenReturn(List.of());

        assertThat(service.deleteInboxBatch(CUTOFF, 100)).isZero();
    }
}
