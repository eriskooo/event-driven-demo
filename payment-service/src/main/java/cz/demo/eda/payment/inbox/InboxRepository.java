package cz.demo.eda.payment.inbox;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Přístup k tabulce inbox. */
@Repository
public class InboxRepository {

    private static final int MAX_ERROR_LENGTH = 2000;
    private static final RowMapper<InboxMessage> MAPPER = (rs, rowNum) -> new InboxMessage(
            rs.getObject("event_id", UUID.class),
            rs.getString("topic"),
            rs.getString("message_key"),
            rs.getString("payload"),
            rs.getString("correlation_id"),
            rs.getInt("attempts"));

    private final JdbcClient jdbc;

    public InboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Uloží přijatou zprávu; vrátí false, pokud zpráva se stejným eventId už v inboxu je (duplicita). */
    public boolean store(UUID eventId, String topic, String key, String payload, String correlationId) {
        Objects.requireNonNull(eventId, "eventId");
        return jdbc.sql("""
                        INSERT INTO inbox (event_id, topic, message_key, payload, correlation_id)
                        VALUES (:eventId, :topic, :key, CAST(:payload AS jsonb), :correlationId)
                        ON CONFLICT (event_id) DO NOTHING""")
                .param("eventId", eventId)
                .param("topic", topic)
                .param("key", key)
                .param("payload", payload)
                .param("correlationId", correlationId)
                .update() == 1;
    }

    /**
     * Zamkne nejstarší čekající zprávu, jejíž čas dalšího pokusu nastal. SKIP LOCKED dovolí
     * paralelní zpracování více replikami. Musí běžet v transakci.
     */
    public Optional<InboxMessage> lockNextDue() {
        return jdbc.sql("""
                        SELECT event_id, topic, message_key, payload::text AS payload, correlation_id, attempts
                          FROM inbox
                         WHERE status = 'PENDING' AND next_attempt_at <= now()
                         ORDER BY received_at
                         LIMIT 1
                           FOR UPDATE SKIP LOCKED""")
                .query(MAPPER)
                .optional();
    }

    /** Označí zprávu jako úspěšně zpracovanou. */
    public void markProcessed(UUID eventId) {
        jdbc.sql("UPDATE inbox SET status = 'PROCESSED', processed_at = now(), last_error = NULL WHERE event_id = :id")
                .param("id", eventId)
                .update();
    }

    /** Naplánuje další pokus o zpracování. */
    public void scheduleRetry(UUID eventId, int attempts, Instant nextAttemptAt, String error) {
        jdbc.sql("""
                        UPDATE inbox SET attempts = :attempts, next_attempt_at = :next, last_error = :error
                         WHERE event_id = :id""")
                .param("id", eventId)
                .param("attempts", attempts)
                .param("next", nextAttemptAt.atOffset(ZoneOffset.UTC))
                .param("error", truncate(error))
                .update();
    }

    /** Označí zprávu jako definitivně neúspěšnou (po vyčerpání pokusů). */
    public void markFailed(UUID eventId, int attempts, String error) {
        jdbc.sql("""
                        UPDATE inbox SET status = 'FAILED', attempts = :attempts, last_error = :error, processed_at = now()
                         WHERE event_id = :id""")
                .param("id", eventId)
                .param("attempts", attempts)
                .param("error", truncate(error))
                .update();
    }

    /**
     * Smaže nejvýše {@code limit} dokončených zpráv (PROCESSED / FAILED) zpracovaných před {@code cutoff}.
     * Čekající zprávy nemaže nikdy. Vrátí počet smazaných řádků.
     */
    public int deleteFinishedBefore(Instant cutoff, int limit) {
        return jdbc.sql("""
                        DELETE FROM inbox
                         WHERE event_id IN (SELECT event_id FROM inbox
                                             WHERE status <> 'PENDING' AND processed_at < :cutoff
                                             LIMIT :limit)""")
                .param("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .param("limit", limit)
                .update();
    }

    /** Vrátí stav zprávy (PENDING / PROCESSED / FAILED), pokud v inboxu je. */
    public Optional<String> findStatus(UUID eventId) {
        return jdbc.sql("SELECT status FROM inbox WHERE event_id = :id").param("id", eventId)
                .query(String.class).optional();
    }

    private static String truncate(String value) {
        return value == null || value.length() <= MAX_ERROR_LENGTH ? value : value.substring(0, MAX_ERROR_LENGTH);
    }
}
