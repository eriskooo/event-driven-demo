package cz.demo.eda.order.cleanup;

import cz.demo.eda.order.inbox.InboxRepository;
import cz.demo.eda.order.outbox.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Mazání starých zpráv z inboxu a outboxu; každá dávka je samostatná krátká transakce. */
@Service
@Transactional
public class CleanupService {

    public static final String DELETED_METRIC = "eda.cleanup.deleted";

    private final InboxRepository inbox;
    private final OutboxRepository outbox;
    private final MeterRegistry registry;

    public CleanupService(InboxRepository inbox, OutboxRepository outbox, MeterRegistry registry) {
        this.inbox = inbox;
        this.outbox = outbox;
        this.registry = registry;
    }

    /**
     * Smaže nejvýše {@code limit} dokončených zpráv inboxu (PROCESSED / FAILED) zpracovaných před
     * {@code cutoff}. Čekající zprávy nemaže nikdy. Vrátí počet smazaných řádků.
     */
    public int deleteInboxBatch(Instant cutoff, int limit) {
        List<UUID> ids = inbox.findFinishedIdsBefore(cutoff, Limit.of(limit));
        inbox.deleteAllByIdInBatch(ids);
        count("inbox", ids.size());
        return ids.size();
    }

    /**
     * Smaže nejvýše {@code limit} publikovaných zpráv outboxu odeslaných před {@code cutoff}.
     * Nepublikované zprávy nemaže nikdy. Vrátí počet smazaných řádků.
     */
    public int deleteOutboxBatch(Instant cutoff, int limit) {
        List<Long> ids = outbox.findPublishedIdsBefore(cutoff, Limit.of(limit));
        outbox.deleteAllByIdInBatch(ids);
        count("outbox", ids.size());
        return ids.size();
    }

    private void count(String table, int deleted) {
        Counter.builder(DELETED_METRIC).tag("table", table).register(registry).increment(deleted);
    }
}
