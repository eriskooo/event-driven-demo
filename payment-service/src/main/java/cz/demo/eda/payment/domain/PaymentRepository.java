package cz.demo.eda.payment.domain;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/** Platby v PostgreSQL (schéma payments). */
@Repository
public class PaymentRepository {

    private static final RowMapper<Payment> MAPPER = (rs, rowNum) -> new Payment(
            rs.getString("id"),
            rs.getString("order_id"),
            PaymentStatus.valueOf(rs.getString("status")),
            rs.getBigDecimal("amount"),
            rs.getString("currency"),
            rs.getString("failure_reason"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant());

    private final JdbcClient jdbc;

    public PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Uloží platbu; druhá platba pro stejnou objednávku poruší UNIQUE(order_id). */
    public Payment save(Payment payment) {
        jdbc.sql("""
                        INSERT INTO payments (id, order_id, status, amount, currency, failure_reason, created_at)
                        VALUES (:id, :orderId, :status, :amount, :currency, :failureReason, :createdAt)""")
                .param("id", payment.id())
                .param("orderId", payment.orderId())
                .param("status", payment.status().name())
                .param("amount", payment.amount())
                .param("currency", payment.currency())
                .param("failureReason", payment.failureReason())
                // PostgreSQL JDBC driver neumí java.time.Instant jako parametr, OffsetDateTime ano.
                .param("createdAt", payment.createdAt().atOffset(ZoneOffset.UTC))
                .update();
        return payment;
    }

    /** Najde platbu podle ID objednávky. */
    public Optional<Payment> findByOrderId(String orderId) {
        if (orderId == null) {
            return Optional.empty();
        }
        return jdbc.sql("SELECT * FROM payments WHERE order_id = :orderId").param("orderId", orderId)
                .query(MAPPER).optional();
    }
}
