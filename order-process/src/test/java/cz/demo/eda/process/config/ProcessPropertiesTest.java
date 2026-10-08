package cz.demo.eda.process.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ProcessPropertiesTest {

    @Test
    @DisplayName("Přijme kladné TTL zprávy")
    void should_keepTtl_whenPositive() {
        assertThat(new ProcessProperties(Duration.ofMinutes(1)).messageTtl()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    @DisplayName("Odmítne nulové TTL – zpráva, která předběhne proces, by se ztratila")
    void should_reject_whenTtlIsZero() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ProcessProperties(Duration.ZERO));
    }

    @Test
    @DisplayName("Odmítne chybějící TTL")
    void should_reject_whenTtlIsNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ProcessProperties(null));
    }
}
