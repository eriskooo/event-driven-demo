package cz.demo.eda.payment.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ProcessedEventStoreTest {

    @Test
    @DisplayName("Nová událost není označena jako zpracovaná")
    void should_returnFalse_whenEventNotSeen() {
        var store = new ProcessedEventStore();

        assertThat(store.isProcessed(UUID.randomUUID())).isFalse();
        assertThat(store.size()).isZero();
    }

    @Test
    @DisplayName("Po označení je událost rozpoznána jako duplicita")
    void should_returnTrue_whenEventMarked() {
        var store = new ProcessedEventStore();
        var id = UUID.randomUUID();

        store.markProcessed(id);

        assertThat(store.isProcessed(id)).isTrue();
    }

    @Test
    @DisplayName("Při překročení kapacity zapomene nejstarší událost")
    void should_evictEldest_whenCapacityExceeded() {
        var store = new ProcessedEventStore(2);
        var first = UUID.randomUUID();
        var second = UUID.randomUUID();
        var third = UUID.randomUUID();

        store.markProcessed(first);
        store.markProcessed(second);
        store.markProcessed(third);

        assertThat(store.size()).isEqualTo(2);
        assertThat(store.isProcessed(first)).isFalse();
        assertThat(store.isProcessed(third)).isTrue();
    }

    @Test
    @DisplayName("Odmítne nulovou kapacitu")
    void should_throw_whenCapacityIsZero() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ProcessedEventStore(0));
    }

    @Test
    @DisplayName("Odmítne null eventId")
    void should_throw_whenEventIdIsNull() {
        var store = new ProcessedEventStore();

        assertThatNullPointerException().isThrownBy(() -> store.isProcessed(null));
        assertThatNullPointerException().isThrownBy(() -> store.markProcessed(null));
    }
}
