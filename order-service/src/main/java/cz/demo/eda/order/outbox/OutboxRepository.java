package cz.demo.eda.order.outbox;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Přístup k tabulce outbox. */
@Repository
public class OutboxRepository {

    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {
    };

    private final JdbcClient jdbc;
    private final JsonMapper jsonMapper;
    private final RowMapper<OutboxMessage> mapper;

    public OutboxRepository(JdbcClient jdbc, JsonMapper jsonMapper) {
        this.jdbc = jdbc;
        this.jsonMapper = jsonMapper;
        this.mapper = (rs, rowNum) -> new OutboxMessage(
                rs.getLong("id"),
                rs.getObject("event_id", UUID.class),
                rs.getString("topic"),
                rs.getString("message_key"),
                rs.getString("payload"),
                jsonMapper.readValue(rs.getString("headers"), HEADERS_TYPE));
    }

    /** Vloží zprávu do outboxu; volá se uvnitř transakce doménové změny. */
    public void insert(UUID eventId, String topic, String key, String payload, Map<String, String> headers) {
        jdbc.sql("""
                        INSERT INTO outbox (event_id, topic, message_key, payload, headers)
                        VALUES (:eventId, :topic, :key, CAST(:payload AS jsonb), CAST(:headers AS jsonb))""")
                .param("eventId", eventId)
                .param("topic", topic)
                .param("key", key)
                .param("payload", payload)
                .param("headers", jsonMapper.writeValueAsString(headers))
                .update();
    }

    /**
     * Zamkne a vrátí nejstarší nepublikované zprávy. SKIP LOCKED umožní běh více replik relaye
     * současně – každá si vezme jinou dávku. Musí běžet v transakci.
     */
    public List<OutboxMessage> lockUnpublished(int limit) {
        return jdbc.sql("""
                        SELECT id, event_id, topic, message_key, payload::text AS payload, headers::text AS headers
                          FROM outbox
                         WHERE published_at IS NULL
                         ORDER BY id
                         LIMIT :limit
                           FOR UPDATE SKIP LOCKED""")
                .param("limit", limit)
                .query(mapper)
                .list();
    }

    /** Označí zprávy jako publikované. */
    public void markPublished(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("UPDATE outbox SET published_at = now() WHERE id IN (:ids)").param("ids", ids).update();
    }

    /**
     * Smaže nejvýše {@code limit} publikovaných zpráv starších než {@code cutoff}.
     * Nepublikované zprávy nemaže nikdy. Vrátí počet smazaných řádků.
     */
    public int deletePublishedBefore(Instant cutoff, int limit) {
        return jdbc.sql("""
                        DELETE FROM outbox
                         WHERE id IN (SELECT id FROM outbox WHERE published_at < :cutoff LIMIT :limit)""")
                .param("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .param("limit", limit)
                .update();
    }

    /** Vrátí počet zpráv čekajících na publikaci. */
    public long countUnpublished() {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Long.class).single();
    }
}
