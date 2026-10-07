package cz.demo.eda.order.inbox;

import cz.demo.eda.order.support.jpa.AssignedIdEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Přijatá zpráva v inboxu včetně stavu zpracování. PK eventId zajišťuje deduplikaci;
 * přechody stavu (zpracováno / retry / selhání) mění jen metody této třídy.
 */
@Entity
@Table(name = "inbox")
public class InboxEntry extends AssignedIdEntity<UUID> {

    static final int MAX_ERROR_LENGTH = 2000;

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(nullable = false, length = 200)
    private String topic;

    @Column(name = "message_key", length = 200)
    private String messageKey;

    /** Událost jako JSON (sloupec jsonb). */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(name = "correlation_id", length = 100)
    private String correlationId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private InboxStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error", length = MAX_ERROR_LENGTH)
    private String lastError;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    /** Pro JPA. */
    protected InboxEntry() {
    }

    private InboxEntry(UUID eventId, String topic, String messageKey, String payload, String correlationId,
                       Instant now) {
        this.eventId = Objects.requireNonNull(eventId, "eventId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.messageKey = messageKey;
        this.payload = Objects.requireNonNull(payload, "payload");
        this.correlationId = correlationId;
        this.status = InboxStatus.PENDING;
        this.nextAttemptAt = now;
        this.receivedAt = now;
    }

    /** Nová přijatá zpráva připravená ke zpracování. */
    public static InboxEntry received(UUID eventId, String topic, String messageKey, String payload,
                                      String correlationId, Instant now) {
        return new InboxEntry(eventId, topic, messageKey, payload, correlationId, now);
    }

    /** Označí zprávu jako úspěšně zpracovanou. */
    public void markProcessed(Instant now) {
        this.status = InboxStatus.PROCESSED;
        this.processedAt = now;
        this.lastError = null;
    }

    /** Zaznamená neúspěšný pokus a naplánuje další. */
    public void scheduleRetry(Instant nextAttemptAt, String error) {
        this.attempts++;
        this.nextAttemptAt = nextAttemptAt;
        this.lastError = truncate(error);
    }

    /** Zaznamená poslední neúspěšný pokus – zpráva se už nebude zpracovávat. */
    public void markFailed(String error, Instant now) {
        this.attempts++;
        this.status = InboxStatus.FAILED;
        this.processedAt = now;
        this.lastError = truncate(error);
    }

    @Override
    public UUID getId() {
        return eventId;
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

    public String correlationId() {
        return correlationId;
    }

    public InboxStatus status() {
        return status;
    }

    public int attempts() {
        return attempts;
    }

    public Instant nextAttemptAt() {
        return nextAttemptAt;
    }

    public String lastError() {
        return lastError;
    }

    public Instant receivedAt() {
        return receivedAt;
    }

    public Instant processedAt() {
        return processedAt;
    }

    private static String truncate(String value) {
        return value == null || value.length() <= MAX_ERROR_LENGTH ? value : value.substring(0, MAX_ERROR_LENGTH);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof InboxEntry other && eventId != null && eventId.equals(other.eventId));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(eventId);
    }

    @Override
    public String toString() {
        return "InboxEntry[eventId=" + eventId + ", topic=" + topic + ", status=" + status + ", attempts=" + attempts + "]";
    }
}
