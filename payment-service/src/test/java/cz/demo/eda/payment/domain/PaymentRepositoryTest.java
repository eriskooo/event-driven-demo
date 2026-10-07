package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.PostgresTestcontainer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresTestcontainer.class, PaymentRepository.class})
class PaymentRepositoryTest {

    private static final Instant NOW = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Autowired
    private PaymentRepository repository;

    @Test
    @DisplayName("Uloženou platbu najde podle ID objednávky")
    void should_findPayment_whenSaved() {
        var payment = repository.save(new Payment("p-1", "o-1", PaymentStatus.COMPLETED, new BigDecimal("10.50"),
                "CZK", null, NOW));

        assertThat(repository.findByOrderId("o-1")).contains(payment);
    }

    @Test
    @DisplayName("Zamítnutou platbu uloží i s důvodem")
    void should_keepFailureReason_whenPaymentFailed() {
        repository.save(new Payment("p-2", "o-2", PaymentStatus.FAILED, BigDecimal.ONE.setScale(2), "EUR",
                "declined", NOW));

        assertThat(repository.findByOrderId("o-2")).get().extracting(Payment::failureReason).isEqualTo("declined");
    }

    @Test
    @DisplayName("Druhou platbu pro stejnou objednávku odmítne")
    void should_reject_whenSecondPaymentForSameOrder() {
        repository.save(new Payment("p-1", "o-1", PaymentStatus.COMPLETED, BigDecimal.TEN, "CZK", null, NOW));

        assertThatThrownBy(() -> repository.save(
                new Payment("p-2", "o-1", PaymentStatus.FAILED, BigDecimal.TEN, "CZK", "x", NOW)))
                .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    @DisplayName("Pro neznámou nebo null objednávku vrátí prázdno")
    void should_returnEmpty_whenOrderUnknownOrNull() {
        assertThat(repository.findByOrderId("missing")).isEmpty();
        assertThat(repository.findByOrderId(null)).isEmpty();
    }
}
