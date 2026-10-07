package cz.demo.eda.order.inbox;

import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;

/**
 * Zpracovává zprávy z inboxu. Každá zpráva má vlastní transakci; handler běží v savepointu (NESTED),
 * takže při chybě se vrátí jen jeho změny a ve stejné transakci se zapíše naplánovaný retry,
 * po vyčerpání pokusů stav FAILED a DLT zpráva do outboxu.
 */
@Component
public class InboxProcessor {

    private static final Logger log = LoggerFactory.getLogger(InboxProcessor.class);

    private final InboxRepository repository;
    private final InboxMessageHandler handler;
    private final OutboxPublisher outbox;
    private final InboxProperties properties;
    private final MessagingMetrics metrics;
    private final TransactionOperations transaction;
    private final TransactionOperations savepoint;
    private final Clock clock;

    @Autowired
    public InboxProcessor(InboxRepository repository, InboxMessageHandler handler, OutboxPublisher outbox,
                          InboxProperties properties, MessagingMetrics metrics,
                          PlatformTransactionManager transactionManager, Clock clock) {
        this(repository, handler, outbox, properties, metrics, new TransactionTemplate(transactionManager),
                nested(transactionManager), clock);
    }

    InboxProcessor(InboxRepository repository, InboxMessageHandler handler, OutboxPublisher outbox,
                   InboxProperties properties, MessagingMetrics metrics, TransactionOperations transaction,
                   TransactionOperations savepoint, Clock clock) {
        this.repository = repository;
        this.handler = handler;
        this.outbox = outbox;
        this.properties = properties;
        this.metrics = metrics;
        this.transaction = transaction;
        this.savepoint = savepoint;
        this.clock = clock;
    }

    /** Zpracuje až batchSize zpráv, jejichž čas zpracování nastal. */
    @Scheduled(fixedDelayString = "${eda.inbox.poll-interval-ms:500}")
    public void processDue() {
        try {
            for (int i = 0; i < properties.batchSize(); i++) {
                if (!Boolean.TRUE.equals(transaction.execute(status -> processNext()))) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            log.error("Inbox processing failed, will retry on next run: {}", e.getMessage(), e);
        }
    }

    /** Zpracuje jednu zprávu; vrátí false, pokud žádná nečeká. Musí běžet v transakci. */
    boolean processNext() {
        var next = repository.lockNextDue();
        if (next.isEmpty()) {
            return false;
        }
        var message = next.get();
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, message.correlationId());
        try {
            savepoint.executeWithoutResult(status -> handler.handle(message));
            repository.markProcessed(message.eventId());
            metrics.inboxProcessed(message.topic(), MessagingMetrics.INBOX_SUCCESS);
        } catch (RuntimeException e) {
            onFailure(message, e);
        } finally {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
        return true;
    }

    private void onFailure(InboxMessage message, RuntimeException e) {
        var attempts = message.attempts() + 1;
        if (attempts >= properties.maxAttempts()) {
            repository.markFailed(message.eventId(), attempts, e.toString());
            outbox.publishDeadLetter(message, attempts, e);
            metrics.inboxProcessed(message.topic(), MessagingMetrics.INBOX_FAILED);
            metrics.deadLettered(Topics.dltOf(message.topic()));
            log.error("Delivery attempt {} of event {} failed, giving up and moving it to {}", attempts,
                    message.eventId(), Topics.dltOf(message.topic()), e);
            return;
        }
        var backoff = properties.backoffAfter(attempts);
        repository.scheduleRetry(message.eventId(), attempts, clock.instant().plus(backoff), e.toString());
        metrics.inboxProcessed(message.topic(), MessagingMetrics.INBOX_RETRY);
        log.warn("Delivery attempt {} of event {} failed, retry in {} ms: {}", attempts, message.eventId(),
                backoff.toMillis(), e.getMessage());
    }

    private static TransactionTemplate nested(PlatformTransactionManager transactionManager) {
        var template = new TransactionTemplate(transactionManager);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);
        return template;
    }
}
