package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.event.PaymentResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentSimulatorTest {

    private static final BigDecimal POISON = new BigDecimal("666");

    @Test
    @DisplayName("Při failure-rate 0 platba vždy projde a nese correlationId i částku")
    void should_complete_whenFailureRateIsZero() {
        PaymentSimulator simulator = simulator(0.0, 0.0);
        ProcessPayment order = order("10.00");

        PaymentResult result = simulator.process(order);

        assertThat(result).isInstanceOfSatisfying(PaymentCompleted.class, completed -> {
            assertThat(completed.orderId()).isEqualTo(order.orderId());
            assertThat(completed.correlationId()).isEqualTo(order.correlationId());
            assertThat(completed.amount()).isEqualByComparingTo("10.00");
            assertThat(completed.paymentId()).isNotBlank();
        });
    }

    @Test
    @DisplayName("Při failure-rate 1 platba vždy selže")
    void should_fail_whenFailureRateIsOne() {
        PaymentResult result = simulator(1.0, 0.999).process(order("10.00"));

        assertThat(result).isInstanceOfSatisfying(PaymentFailed.class,
                failed -> assertThat(failed.reason()).isEqualTo(PaymentSimulator.DECLINED_REASON));
    }

    @ParameterizedTest(name = "rate={0}, random={1} → selže={2}")
    @CsvSource({"0.2, 0.19, true", "0.2, 0.2, false", "0.2, 0.0, true", "0.0, 0.0, false"})
    @DisplayName("Hranice failure-rate: selže jen když náhodná hodnota je ostře menší")
    void should_respectBoundary_whenRandomNearFailureRate(double rate, double randomValue, boolean expectFailure) {
        PaymentResult result = simulator(rate, randomValue).process(order("1"));

        assertThat(result instanceof PaymentFailed).isEqualTo(expectFailure);
    }

    @Test
    @DisplayName("Poison částka vyvolá technickou výjimku bez ohledu na scale")
    void should_throw_whenAmountIsPoison() {
        PaymentSimulator simulator = simulator(0.0, 0.0);

        assertThatThrownBy(() -> simulator.process(order("666.00")))
                .isInstanceOf(PaymentProcessingException.class)
                .hasMessageContaining("666");
    }

    private static PaymentSimulator simulator(double failureRate, double randomValue) {
        RandomGenerator fixedRandom = new RandomGenerator() {
            @Override
            public long nextLong() {
                return 0;
            }

            @Override
            public double nextDouble() {
                return randomValue;
            }
        };
        return new PaymentSimulator(new PaymentProperties(failureRate, POISON), fixedRandom);
    }

    private static ProcessPayment order(String amount) {
        return ProcessPayment.of("corr-1", "o-1", new BigDecimal(amount), "CZK");
    }
}
