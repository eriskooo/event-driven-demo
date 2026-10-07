package cz.demo.eda.order.domain;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** Objednávky v PostgreSQL (schéma orders). */
@Repository
public class OrderRepository {

    private static final RowMapper<Order> ORDER_MAPPER = (rs, rowNum) -> new Order(
            rs.getString("id"),
            rs.getString("customer_id"),
            rs.getBigDecimal("amount"),
            rs.getString("currency"),
            OrderStatus.valueOf(rs.getString("status")),
            rs.getString("payment_id"),
            rs.getString("failure_reason"),
            rs.getObject("created_at", OffsetDateTime.class).toInstant(),
            rs.getObject("updated_at", OffsetDateTime.class).toInstant());

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Vloží novou objednávku a vrátí ji. */
    public Order save(Order order) {
        jdbc.sql("""
                        INSERT INTO orders (id, customer_id, amount, currency, status, payment_id, failure_reason,
                                            created_at, updated_at)
                        VALUES (:id, :customerId, :amount, :currency, :status, :paymentId, :failureReason,
                                :createdAt, :updatedAt)""")
                .param("id", order.id())
                .param("customerId", order.customerId())
                .param("amount", order.amount())
                .param("currency", order.currency())
                .param("status", order.status().name())
                .param("paymentId", order.paymentId())
                .param("failureReason", order.failureReason())
                .param("createdAt", utc(order.createdAt()))
                .param("updatedAt", utc(order.updatedAt()))
                .update();
        return order;
    }

    /** Najde objednávku podle ID. */
    public Optional<Order> findById(String id) {
        if (id == null) {
            return Optional.empty();
        }
        return jdbc.sql("SELECT * FROM orders WHERE id = :id").param("id", id).query(ORDER_MAPPER).optional();
    }

    /**
     * Pod zámkem řádku (SELECT ... FOR UPDATE) aplikuje změnu na existující objednávku.
     * Prázdný výsledek znamená neznámé ID.
     */
    @Transactional
    public Optional<Order> update(String id, UnaryOperator<Order> change) {
        if (id == null) {
            return Optional.empty();
        }
        return jdbc.sql("SELECT * FROM orders WHERE id = :id FOR UPDATE").param("id", id)
                .query(ORDER_MAPPER).optional()
                .map(change)
                .map(this::updateState);
    }

    /** Vrátí počet uložených objednávek. */
    public long count() {
        return jdbc.sql("SELECT count(*) FROM orders").query(Long.class).single();
    }

    private Order updateState(Order order) {
        jdbc.sql("""
                        UPDATE orders
                           SET status = :status, payment_id = :paymentId, failure_reason = :failureReason,
                               updated_at = :updatedAt
                         WHERE id = :id""")
                .param("id", order.id())
                .param("status", order.status().name())
                .param("paymentId", order.paymentId())
                .param("failureReason", order.failureReason())
                .param("updatedAt", utc(order.updatedAt()))
                .update();
        return order;
    }

    // PostgreSQL JDBC driver neumí java.time.Instant jako parametr, OffsetDateTime ano.
    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
