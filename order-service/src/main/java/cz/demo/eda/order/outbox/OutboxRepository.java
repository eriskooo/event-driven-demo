package cz.demo.eda.order.outbox;

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

/** Outbox – odchozí zprávy čekající na odeslání do Kafky. */
@Transactional(readOnly = true)
public interface OutboxRepository extends JpaRepository<OutboxEntry, Long> {

    /** jakarta.persistence.lock.timeout = -2 → Hibernate vygeneruje FOR UPDATE SKIP LOCKED. */
    String SKIP_LOCKED = "-2";

    /**
     * Zamkne nejstarší nepublikované zprávy. SKIP LOCKED umožní běh více replik relaye současně –
     * každá si vezme jinou dávku. Volá se v transakci servisu.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED))
    @Query("select e from OutboxEntry e where e.publishedAt is null order by e.id")
    List<OutboxEntry> lockUnpublished(Limit limit);

    /** Vrátí počet zpráv čekajících na publikaci. */
    long countByPublishedAtIsNull();

    /** ID publikovaných zpráv odeslaných před cutoff. */
    @Query("select e.id from OutboxEntry e where e.publishedAt < :cutoff")
    List<Long> findPublishedIdsBefore(Instant cutoff, Limit limit);
}
