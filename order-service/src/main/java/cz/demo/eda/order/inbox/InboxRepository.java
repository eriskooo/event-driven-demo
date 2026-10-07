package cz.demo.eda.order.inbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Inbox – přijaté zprávy čekající na zpracování. */
@Transactional(readOnly = true)
public interface InboxRepository extends JpaRepository<InboxEntry, UUID> {

    /** jakarta.persistence.lock.timeout = -2 → Hibernate vygeneruje FOR UPDATE SKIP LOCKED. */
    String SKIP_LOCKED = "-2";

    /** ID čekajících zpráv, jejichž čas pokusu nastal, od nejstarší. */
    @Query("""
            select e.eventId from InboxEntry e
             where e.status = cz.demo.eda.order.inbox.InboxStatus.PENDING and e.nextAttemptAt <= :now
             order by e.receivedAt""")
    List<UUID> findDueIds(Instant now, Limit limit);

    /**
     * Zamkne zprávu, pokud stále čeká a její čas nastal. SKIP LOCKED: zprávu, kterou právě zpracovává
     * jiná replika, přeskočí (prázdný výsledek) místo čekání na zámek.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("""
            select e from InboxEntry e
             where e.eventId = :eventId
               and e.status = cz.demo.eda.order.inbox.InboxStatus.PENDING and e.nextAttemptAt <= :now""")
    Optional<InboxEntry> lockIfDue(UUID eventId, Instant now);

    /** Načte zprávu se zámkem řádku (čeká, dokud ho jiná transakce neuvolní). */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select e from InboxEntry e where e.eventId = :eventId")
    Optional<InboxEntry> findForUpdate(UUID eventId);

    /** ID dokončených zpráv (PROCESSED / FAILED) zpracovaných před cutoff. */
    @Query("""
            select e.eventId from InboxEntry e
             where e.status <> cz.demo.eda.order.inbox.InboxStatus.PENDING and e.processedAt < :cutoff""")
    List<UUID> findFinishedIdsBefore(Instant cutoff, Limit limit);
}
