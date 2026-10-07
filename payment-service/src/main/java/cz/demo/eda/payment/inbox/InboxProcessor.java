package cz.demo.eda.payment.inbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Plánovač zpracování inboxu. Sám transakce neřídí – každé volání {@link InboxService} je samostatná
 * transakce; po neúspěšném zpracování se v nové transakci zaznamená retry nebo DLT.
 */
@Component
public class InboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessor.class);

    private final InboxService inbox;

    public InboxProcessor(InboxService inbox) {
        this.inbox = inbox;
    }

    /** Zpracuje dávku zpráv, jejichž čas zpracování nastal. */
    @Scheduled(fixedDelayString = "${eda.inbox.poll-interval-ms:500}")
    public void processDue() {
        try {
            inbox.findDueIds().forEach(this::processOne);
        } catch (RuntimeException e) {
            log.error("Inbox processing failed, will retry on next run: {}", e.getMessage(), e);
        }
    }

    private void processOne(UUID eventId) {
        try {
            inbox.process(eventId);
        } catch (RuntimeException e) {
            // Transakce zpracování je už vrácená; neúspěšný pokus se zapíše v nové transakci.
            inbox.recordFailure(eventId, e);
        }
    }
}
