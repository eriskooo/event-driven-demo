package cz.demo.eda.order.inbox;

import cz.demo.eda.order.outbox.OutboxPublisher;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Inbox: ukládání přijatých zpráv a jejich zpracování. Každá veřejná metoda je samostatná transakce –
 * zpracování zprávy (doménová změna + outbox + stav inboxu) se buď celé potvrdí, nebo celé vrátí.
 */
@Service
@Transactional
public class InboxService {

    private static final Logger log = LoggerFactory.getLogger(InboxService.class);

    private final InboxRepository repository;
    private final InboxMessageHandler handler;
    private final OutboxPublisher outbox;
    private final InboxProperties properties;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public InboxService(InboxRepository repository, InboxMessageHandler handler, OutboxPublisher outbox,
                        InboxProperties properties, MessagingMetrics metrics, Clock clock) {
        this.repository = repository;
        this.handler = handler;
        this.outbox = outbox;
        this.properties = properties;
        this.metrics = metrics;
        this.clock = clock;
    }

    /**
     * Uloží přijatou zprávu, pokud ještě v inboxu není; vrátí false pro duplicitu. Souběžný zápis
     * stejného eventId skončí porušením PK – výjimka vrátí zprávu Kafce a při dalším doručení se pozná duplicita.
     */
    public boolean store(InboxEntry entry) {
        if (repository.existsById(entry.eventId())) {
            return false;
        }
        repository.save(entry);
        return true;
    }

    /** ID zpráv připravených ke zpracování (nejvýše batchSize, od nejstarší). */
    @Transactional(readOnly = true)
    public List<UUID> findDueIds() {
        return repository.findDueIds(clock.instant(), Limit.of(properties.batchSize()));
    }

    /**
     * Zpracuje zprávu, pokud stále čeká a nezpracovává ji jiná replika. Výjimka handleru vrátí celou
     * transakci (vč. doménových změn) – volající ji pak zaznamená přes {@link #recordFailure}.
     *
     * @return false, pokud zpráva už není k dispozici (zpracovaná nebo zamčená jinde)
     */
    public boolean process(UUID eventId) {
        Optional<InboxEntry> locked = repository.lockIfDue(eventId, clock.instant());
        if (locked.isEmpty()) {
            return false;
        }
        InboxEntry entry = locked.get();
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, entry.correlationId());
        try {
            handler.handle(entry);
            entry.markProcessed(clock.instant());
            metrics.inboxProcessed(entry.topic(), MessagingMetrics.INBOX_SUCCESS);
            return true;
        } finally {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
    }

    /**
     * Zaznamená neúspěšný pokus: naplánuje retry s exponenciálním backoffem, po vyčerpání pokusů
     * označí zprávu FAILED a zařadí ji do &lt;topic&gt;.DLT (přes outbox, tedy ve stejné transakci).
     */
    public void recordFailure(UUID eventId, Exception cause) {
        InboxEntry entry = repository.findForUpdate(eventId).orElseThrow();
        if (entry.status() != InboxStatus.PENDING) {
            return;
        }
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, entry.correlationId());
        try {
            scheduleRetryOrFail(entry, cause);
        } finally {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
    }

    private void scheduleRetryOrFail(InboxEntry entry, Exception cause) {
        UUID eventId = entry.eventId();
        int attempts = entry.attempts() + 1;
        if (attempts >= properties.maxAttempts()) {
            entry.markFailed(cause.toString(), clock.instant());
            outbox.publishDeadLetter(entry, cause);
            metrics.inboxProcessed(entry.topic(), MessagingMetrics.INBOX_FAILED);
            metrics.deadLettered(Topics.dltOf(entry.topic()));
            log.error("Delivery attempt {} of event {} failed, giving up and moving it to {}", attempts,
                    eventId, Topics.dltOf(entry.topic()), cause);
            return;
        }
        Duration backoff = properties.backoffAfter(attempts);
        entry.scheduleRetry(clock.instant().plus(backoff), cause.toString());
        metrics.inboxProcessed(entry.topic(), MessagingMetrics.INBOX_RETRY);
        log.warn("Delivery attempt {} of event {} failed, retry in {} ms: {}", attempts, eventId,
                backoff.toMillis(), cause.getMessage());
    }
}
