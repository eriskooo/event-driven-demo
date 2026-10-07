package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.event.PaymentResult;
import cz.demo.eda.payment.support.jpa.AssignedIdEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Záznam o zpracované platbě; jedna platba na objednávku (UNIQUE order_id). */
@Entity
@Table(name = "payments")
public class Payment extends AssignedIdEntity<String> {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "order_id", nullable = false, unique = true, length = 36)
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(nullable = false, precision = 14, scale = 2)
    private BigDecimal amount;

    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "failure_reason", length = 500)
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /** Pro JPA. */
    protected Payment() {
    }

    Payment(String id, String orderId, PaymentStatus status, BigDecimal amount, String currency,
            String failureReason, Instant createdAt) {
        this.id = Objects.requireNonNull(id, "id");
        this.orderId = Objects.requireNonNull(orderId, "orderId");
        this.status = Objects.requireNonNull(status, "status");
        this.amount = amount;
        this.currency = currency;
        this.failureReason = failureReason;
        this.createdAt = createdAt;
    }

    /** Sestaví záznam platby z objednávky a výsledku simulace. */
    public static Payment of(OrderCreated order, PaymentResult result, Instant now) {
        return switch (result) {
            case PaymentCompleted completed -> new Payment(completed.paymentId(), order.orderId(),
                    PaymentStatus.COMPLETED, order.amount(), order.currency(), null, now);
            case PaymentFailed failed -> new Payment(UUID.randomUUID().toString(), order.orderId(),
                    PaymentStatus.FAILED, order.amount(), order.currency(), failed.reason(), now);
        };
    }

    @Override
    public String getId() {
        return id;
    }

    public String id() {
        return id;
    }

    public String orderId() {
        return orderId;
    }

    public PaymentStatus status() {
        return status;
    }

    public BigDecimal amount() {
        return amount;
    }

    public String currency() {
        return currency;
    }

    public String failureReason() {
        return failureReason;
    }

    public Instant createdAt() {
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Payment other && id != null && id.equals(other.id));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "Payment[id=" + id + ", orderId=" + orderId + ", status=" + status + "]";
    }
}
