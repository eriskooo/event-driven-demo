package cz.demo.eda.order.inbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class InboxPropertiesTest {

    private final InboxProperties properties =
            new InboxProperties(4, Duration.ofMillis(500), 2.0, Duration.ofSeconds(5), 50);

    @ParameterizedTest(name = "po {0} neúspěšných pokusech čekat {1} ms")
    @CsvSource({"0, 500", "1, 500", "2, 1000", "3, 2000", "4, 4000", "5, 5000", "20, 5000"})
    @DisplayName("Exponenciální backoff roste násobitelem a je omezen maximem")
    void should_growExponentiallyAndCap_whenAttemptsIncrease(int failedAttempts, long expectedMillis) {
        assertThat(properties.backoffAfter(failedAttempts)).isEqualTo(Duration.ofMillis(expectedMillis));
    }
}
