package cz.demo.eda.order.cleanup;

import cz.demo.eda.order.inbox.InboxRepository;
import cz.demo.eda.order.outbox.OutboxRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiFunction;

/**
 * Periodicky maže dokončené zprávy z inboxu a publikované zprávy z outboxu starší než retence.
 * Maže po dávkách (každý DELETE je vlastní krátká transakce); běh na více replikách je neškodný.
 */
@Component
public class RetentionCleanup {

    public static final String DELETED_METRIC = "eda.cleanup.deleted";
    private static final Logger log = LoggerFactory.getLogger(RetentionCleanup.class);

    private final InboxRepository inbox;
    private final OutboxRepository outbox;
    private final CleanupProperties properties;
    private final MeterRegistry registry;
    private final Clock clock;

    public RetentionCleanup(InboxRepository inbox, OutboxRepository outbox, CleanupProperties properties,
                            MeterRegistry registry, Clock clock) {
        this.inbox = inbox;
        this.outbox = outbox;
        this.properties = properties;
        this.registry = registry;
        this.clock = clock;
    }

    /** Smaže zprávy starší než retence a vrátí celkový počet smazaných řádků. */
    @Scheduled(cron = "${eda.cleanup.cron:0 0 3 * * *}")
    public int purge() {
        var cutoff = clock.instant().minus(Duration.ofDays(properties.retentionDays()));
        try {
            var inboxDeleted = deleteInBatches("inbox", cutoff, inbox::deleteFinishedBefore);
            var outboxDeleted = deleteInBatches("outbox", cutoff, outbox::deletePublishedBefore);
            log.info("Retention cleanup removed {} inbox and {} outbox rows older than {} ({} days)",
                    inboxDeleted, outboxDeleted, cutoff, properties.retentionDays());
            return inboxDeleted + outboxDeleted;
        } catch (RuntimeException e) {
            log.error("Retention cleanup failed, will retry on next schedule: {}", e.getMessage(), e);
            return 0;
        }
    }

    private int deleteInBatches(String table, Instant cutoff, BiFunction<Instant, Integer, Integer> delete) {
        var total = 0;
        int deleted;
        do {
            deleted = delete.apply(cutoff, properties.batchSize());
            total += deleted;
        } while (deleted == properties.batchSize());
        Counter.builder(DELETED_METRIC).tag("table", table).register(registry).increment(total);
        return total;
    }
}
