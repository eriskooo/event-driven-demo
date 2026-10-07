package cz.demo.eda.order.domain;

import cz.demo.eda.order.support.jpa.AssignedIdEntity;
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

/** Objednávka; stav mění jen doménové metody, persistence přes dirty checking. */
@Entity
@Table(name = "orders")
public class Order extends AssignedIdEntity<String> {

    @Id
    @Column(length = 36)
    private String id;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(nullable = false, precision = 14, scale = 2)
    private BigDecimal amount;

    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private OrderStatus status;

    @Column(name = "payment_id", length = 64)
    private String paymentId;

    @Column(name = "failure_reason", length = 500)
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /** Pro JPA. */
    protected Order() {
    }

    private Order(String id, String customerId, BigDecimal amount, String currency, Instant now) {
        this.id = Objects.requireNonNull(id, "id");
        this.customerId = customerId;
        this.amount = Objects.requireNonNull(amount, "amount");
        this.currency = currency;
        this.status = OrderStatus.PENDING_PAYMENT;
        this.createdAt = now;
        this.updatedAt = now;
    }

    /** Založí novou objednávku čekající na platbu. */
    public static Order create(String id, String customerId, BigDecimal amount, String currency, Instant now) {
        return new Order(id, customerId, amount, currency, now);
    }

    /** Označí objednávku jako zaplacenou. */
    public void markPaid(String paymentId, Instant now) {
        this.status = OrderStatus.PAID;
        this.paymentId = paymentId;
        this.failureReason = null;
        this.updatedAt = now;
    }

    /** Označí objednávku jako neúspěšně zaplacenou s uvedeným důvodem. */
    public void markPaymentFailed(String reason, Instant now) {
        this.status = OrderStatus.PAYMENT_FAILED;
        this.paymentId = null;
        this.failureReason = reason;
        this.updatedAt = now;
    }

    @Override
    public String getId() {
        return id;
    }

    public String id() {
        return id;
    }

    public String customerId() {
        return customerId;
    }

    public BigDecimal amount() {
        return amount;
    }

    public String currency() {
        return currency;
    }

    public OrderStatus status() {
        return status;
    }

    public String paymentId() {
        return paymentId;
    }

    public String failureReason() {
        return failureReason;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Order other && id != null && id.equals(other.id));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return "Order[id=" + id + ", status=" + status + "]";
    }
}
