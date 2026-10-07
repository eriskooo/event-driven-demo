package cz.demo.eda.order.domain;

import cz.demo.eda.order.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(PostgresTestcontainer.class)
class OrderRepositoryTest {

    // PostgreSQL ukládá mikrosekundy – porovnáváme s oříznutým časem.
    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private OrderRepository repository;
    @Autowired
    private TestEntityManager em;

    @Test
    @DisplayName("Uloženou objednávku načte z DB se všemi poli")
    void should_loadAllFields_whenSaved() {
        repository.save(Order.create("o-1", "c-1", new BigDecimal("10.50"), "CZK", NOW));
        em.flush();
        em.clear();

        assertThat(repository.findById("o-1")).get().satisfies(o -> {
            assertThat(o.customerId()).isEqualTo("c-1");
            assertThat(o.amount()).isEqualByComparingTo("10.50");
            assertThat(o.currency()).isEqualTo("CZK");
            assertThat(o.status()).isEqualTo(OrderStatus.PENDING_PAYMENT);
            assertThat(o.createdAt()).isEqualTo(NOW);
            assertThat(o.isNew()).isFalse();
        });
    }

    @Test
    @DisplayName("Změna stavu se uloží dirty checkingem")
    void should_persistStateChange_whenEntityModified() {
        repository.save(Order.create("o-1", "c-1", BigDecimal.TEN, "CZK", NOW));
        em.flush();
        em.clear();

        repository.findForUpdate("o-1").orElseThrow().markPaid("p-1", NOW.plusSeconds(5));
        em.flush();
        em.clear();

        assertThat(repository.findById("o-1")).get().satisfies(o -> {
            assertThat(o.status()).isEqualTo(OrderStatus.PAID);
            assertThat(o.paymentId()).isEqualTo("p-1");
            assertThat(o.updatedAt()).isEqualTo(NOW.plusSeconds(5));
        });
    }

    @Test
    @DisplayName("Zámek pro neznámé ID vrátí prázdný výsledek")
    void should_returnEmpty_whenLockingUnknownId() {
        assertThat(repository.findForUpdate("missing")).isEmpty();
        assertThat(repository.findById("missing")).isEmpty();
    }

    @Test
    @DisplayName("Počet objednávek odpovídá uloženým záznamům")
    void should_countOrders_whenSaved() {
        repository.save(Order.create("o-1", "c-1", BigDecimal.ONE, "CZK", NOW));
        repository.save(Order.create("o-2", "c-1", BigDecimal.ONE, "EUR", NOW));
        em.flush();

        assertThat(repository.count()).isEqualTo(2);
    }
}
