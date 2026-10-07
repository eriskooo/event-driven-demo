package cz.demo.eda.payment.cleanup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiFunction;

/**
 * Plánovač úklidu: periodicky maže dokončené zprávy z inboxu a publikované zprávy z outboxu
 * starší než retence. Maže po dávkách přes {@link CleanupService}; běh na více replikách je neškodný.
 */
@Component
public class RetentionCleanup {

    private static final Logger log = LoggerFactory.getLogger(RetentionCleanup.class);

    private final CleanupService cleanup;
    private final CleanupProperties properties;
    private final Clock clock;

    public RetentionCleanup(CleanupService cleanup, CleanupProperties properties, Clock clock) {
        this.cleanup = cleanup;
        this.properties = properties;
        this.clock = clock;
    }

    /** Smaže zprávy starší než retence a vrátí celkový počet smazaných řádků. */
    @Scheduled(cron = "${eda.cleanup.cron:0 0 3 * * *}")
    public int purge() {
        Instant cutoff = clock.instant().minus(Duration.ofDays(properties.retentionDays()));
        try {
            int inboxDeleted = deleteInBatches(cutoff, cleanup::deleteInboxBatch);
            int outboxDeleted = deleteInBatches(cutoff, cleanup::deleteOutboxBatch);
            log.info("Retention cleanup removed {} inbox and {} outbox rows older than {} ({} days)",
                    inboxDeleted, outboxDeleted, cutoff, properties.retentionDays());
            return inboxDeleted + outboxDeleted;
        } catch (RuntimeException e) {
            log.error("Retention cleanup failed, will retry on next schedule: {}", e.getMessage(), e);
            return 0;
        }
    }

    private int deleteInBatches(Instant cutoff, BiFunction<Instant, Integer, Integer> deleteBatch) {
        int total = 0;
        int deleted;
        do {
            deleted = deleteBatch.apply(cutoff, properties.batchSize());
            total += deleted;
        } while (deleted == properties.batchSize());
        return total;
    }
}
