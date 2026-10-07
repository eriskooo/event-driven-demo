package cz.demo.eda.order.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Plánovač outboxu: periodicky odesílá dávky; každá dávka je samostatná transakce {@link OutboxService}. */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxService outbox;

    public OutboxRelay(OutboxService outbox) {
        this.outbox = outbox;
    }

    /** Odešle všechny čekající zprávy po dávkách; chyba se zaloguje a zkusí se znovu při dalším běhu. */
    @Scheduled(fixedDelayString = "${eda.outbox.poll-interval-ms:500}")
    public void publishPending() {
        try {
            int sent;
            do {
                sent = outbox.publishBatch();
            } while (sent == OutboxService.BATCH_SIZE);
        } catch (RuntimeException e) {
            log.error("Outbox relay failed, pending messages will be retried: {}", e.getMessage(), e);
        }
    }
}
