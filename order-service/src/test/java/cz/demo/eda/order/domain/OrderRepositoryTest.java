package cz.demo.eda.order.domain;

import cz.demo.eda.order.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresTestcontainer.class, OrderRepository.class})
class OrderRepositoryTest {

    // PostgreSQL ukládá mikrosekundy – porovnáváme s oříznutým časem.
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private OrderRepository repository;

    @Test
    @DisplayName("Uloženou objednávku najde podle ID se všemi poli")
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
    @DisplayName("Update uloží nový stav, paymentId i čas změny")
    void should_persistChange_whenOrderExists() {
        repository.save(order("o-1"));
        var later = NOW.plusSeconds(5);

        var updated = repository.update("o-1", o -> o.markPaid("p-1", later));

        assertThat(updated).get().extracting(Order::status).isEqualTo(OrderStatus.PAID);
        assertThat(repository.findById("o-1")).get().satisfies(o -> {
            assertThat(o.status()).isEqualTo(OrderStatus.PAID);
            assertThat(o.paymentId()).isEqualTo("p-1");
            assertThat(o.updatedAt()).isEqualTo(later);
            assertThat(o.createdAt()).isEqualTo(NOW);
        });
    }

    @Test
    @DisplayName("Update neznámé objednávky nic nevytvoří")
    void should_notCreateOrder_whenUpdatingUnknownId() {
        assertThat(repository.update("o-x", o -> o)).isEmpty();
        assertThat(repository.count()).isZero();
    }

    @Test
    @DisplayName("Částka se zachová na dvě desetinná místa")
    void should_keepAmountScale_whenSaved() {
        repository.save(Order.create("o-2", "c-1", new BigDecimal("0.01"), "EUR", NOW));

        assertThat(repository.findById("o-2")).get().extracting(Order::amount).isEqualTo(new BigDecimal("0.01"));
    }

    private static Order order(String id) {
        return Order.create(id, "c-1", new BigDecimal("10.00"), "CZK", NOW);
    }
}
