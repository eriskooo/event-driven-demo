package cz.demo.eda.order.domain;

import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/** In-memory úložiště objednávek – demo záměrně nepoužívá databázi. */
@Repository
public class OrderRepository {

    private final Map<String, Order> orders = new ConcurrentHashMap<>();

    /** Uloží (nebo přepíše) objednávku a vrátí ji. */
    public Order save(Order order) {
        orders.put(order.id(), order);
        return order;
    }

    /** Najde objednávku podle ID. */
    public Optional<Order> findById(String id) {
        return id == null ? Optional.empty() : Optional.ofNullable(orders.get(id));
    }

    /** Atomicky aplikuje změnu na existující objednávku; prázdný výsledek znamená neznámé ID. */
    public Optional<Order> update(String id, UnaryOperator<Order> change) {
        return id == null ? Optional.empty() : Optional.ofNullable(orders.computeIfPresent(id, (key, order) -> change.apply(order)));
    }

    /** Vrátí počet uložených objednávek. */
    public int count() {
        return orders.size();
    }
}
