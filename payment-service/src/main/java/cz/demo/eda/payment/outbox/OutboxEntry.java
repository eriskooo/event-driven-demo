package cz.demo.eda.payment.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Odchozí zpráva v outboxu; zapisuje se ve stejné transakci jako doménová změna. */
@Entity
@Table(name = "outbox")
public class OutboxEntry {

    /** BIGSERIAL – pořadí vložení určuje pořadí odeslání. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false, length = 200)
    private String topic;

    @Column(name = "message_key", nullable = false, length = 200)
    private String messageKey;

    /** Zpráva serializovaná do JSON už při zápisu – relay ji posílá beze změny. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    /** Kafka hlavičky (correlationId, u DLT důvod selhání). */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private Map<String, String> headers;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    /** Pro JPA. */
    protected OutboxEntry() {
    }

    private OutboxEntry(UUID eventId, String topic, String messageKey, String payload, Map<String, String> headers,
                        Instant now) {
        this.eventId = Objects.requireNonNull(eventId, "eventId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.messageKey = Objects.requireNonNull(messageKey, "messageKey");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.headers = headers == null ? new HashMap<>() : new HashMap<>(headers);
        this.createdAt = now;
    }

    /** Nová zpráva čekající na odeslání. */
    public static OutboxEntry pending(UUID eventId, String topic, String messageKey, String payload,
                                      Map<String, String> headers, Instant now) {
        return new OutboxEntry(eventId, topic, messageKey, payload, headers, now);
    }

    /** Označí zprávu jako potvrzenou brokerem. */
    public void markPublished(Instant now) {
        this.publishedAt = now;
    }

    public Long id() {
        return id;
    }

    public UUID eventId() {
        return eventId;
    }

    public String topic() {
        return topic;
    }

    public String messageKey() {
        return messageKey;
    }

    public String payload() {
        return payload;
    }

    public Map<String, String> headers() {
        return Map.copyOf(headers);
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant publishedAt() {
        return publishedAt;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof OutboxEntry other && eventId != null && eventId.equals(other.eventId));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }

    @Override
    public String toString() {
        return "OutboxEntry[id=" + id + ", eventId=" + eventId + ", topic=" + topic + ", published=" + (publishedAt != null) + "]";
    }
}
