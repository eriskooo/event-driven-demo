package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(PostgresTestcontainer.class)
class PaymentRepositoryTest {

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private PaymentRepository repository;
    @Autowired
    private TestEntityManager em;

    @Test
    @DisplayName("Uloženou platbu načte z DB podle ID objednávky se všemi poli")
    void should_findPayment_whenSaved() {
        repository.save(new Payment("p-1", "o-1", PaymentStatus.COMPLETED, new BigDecimal("10.50"), "CZK", null, NOW));
        em.flush();
        em.clear();

        assertThat(repository.findByOrderId("o-1")).get().satisfies(p -> {
            assertThat(p.id()).isEqualTo("p-1");
            assertThat(p.status()).isEqualTo(PaymentStatus.COMPLETED);
            assertThat(p.amount()).isEqualByComparingTo("10.50");
            assertThat(p.currency()).isEqualTo("CZK");
            assertThat(p.createdAt()).isEqualTo(NOW);
        });
    }

    @Test
    @DisplayName("Zamítnutou platbu uloží i s důvodem")
    void should_keepFailureReason_whenPaymentFailed() {
        repository.save(new Payment("p-2", "o-2", PaymentStatus.FAILED, BigDecimal.ONE, "EUR", "declined", NOW));
        em.flush();
        em.clear();

        assertThat(repository.findByOrderId("o-2")).get().extracting(Payment::failureReason).isEqualTo("declined");
    }

    @Test
    @DisplayName("Druhou platbu pro stejnou objednávku odmítne")
    void should_reject_whenSecondPaymentForSameOrder() {
        repository.save(new Payment("p-1", "o-1", PaymentStatus.COMPLETED, BigDecimal.TEN, "CZK", null, NOW));
        em.flush();

        repository.save(new Payment("p-2", "o-1", PaymentStatus.FAILED, BigDecimal.TEN, "CZK", "x", NOW));
        assertThatThrownBy(() -> repository.flush()).isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("Pro neznámou objednávku vrátí prázdno")
    void should_returnEmpty_whenOrderUnknown() {
        assertThat(repository.findByOrderId("missing")).isEmpty();
    }
}
