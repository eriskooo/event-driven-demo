package cz.demo.eda.order.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class OrderRepositoryTest {

    private final OrderRepository repository = new OrderRepository();

    @Test
    @DisplayName("Uloženou objednávku najde podle ID")
    void should_findOrder_whenSaved() {
        var order = repository.save(order("o-1"));

        assertThat(repository.findById("o-1")).contains(order);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Pro neznámé ID vrátí prázdný výsledek")
    void should_returnEmpty_whenIdUnknown() {
        assertThat(repository.findById("missing")).isEmpty();
    }

    @Test
    @DisplayName("Pro null ID vrátí prázdný výsledek")
    void should_returnEmpty_whenIdIsNull() {
        assertThat(repository.findById(null)).isEmpty();
        assertThat(repository.update(null, o -> o)).isEmpty();
    }

    @Test
    @DisplayName("Update změní existující objednávku")
    void should_applyChange_whenOrderExists() {
        repository.save(order("o-1"));

        var updated = repository.update("o-1", o -> o.markPaid("p-1", Instant.EPOCH));

        assertThat(updated).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
        assertThat(repository.findById("o-1")).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
    }

    @Test
    @DisplayName("Update neznámé objednávky nic nevytvoří")
    void should_notCreateOrder_whenUpdatingUnknownId() {
        assertThat(repository.update("o-x", o -> o)).isEmpty();
        assertThat(repository.count()).isZero();
    }

    private static Order order(String id) {
        return Order.create(id, "c-1", BigDecimal.TEN, "CZK", Instant.EPOCH);
    }
}
