package cz.demo.eda.order.messaging;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.support.ProcessedEventStore;
import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.support.MessagingMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultListenerTest {

    @Mock
    private OrderService orderService;

    private SimpleMeterRegistry registry;
    private ProcessedEventStore store;
    private PaymentResultListener listener;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        store = new ProcessedEventStore();
        listener = new PaymentResultListener(orderService, store, new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Výsledek platby předá doméně a označí jako zpracovaný")
    void should_applyResult_whenFirstDelivery() {
        var event = PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE);

        listener.onPaymentResult(event);

        verify(orderService).applyPaymentResult(event);
        assertThat(store.isProcessed(event.eventId())).isTrue();
        assertThat(consumed(MessagingMetrics.OUTCOME_PROCESSED)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Duplicitní doručení stejného eventId zpracuje jen jednou")
    void should_skipDuplicate_whenSameEventDeliveredTwice() {
        var event = PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE);

        listener.onPaymentResult(event);
        listener.onPaymentResult(event);

        verify(orderService, times(1)).applyPaymentResult(event);
        assertThat(consumed(MessagingMetrics.OUTCOME_DUPLICATE)).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Při chybě zpracování událost neoznačí, aby ji retry zpracoval znovu")
    void should_notMarkProcessed_whenProcessingFails() {
        var event = PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE);
        when(orderService.applyPaymentResult(any())).thenThrow(new IllegalStateException("boom"));

        assertThatIllegalStateException().isThrownBy(() -> listener.onPaymentResult(event));

        assertThat(store.isProcessed(event.eventId())).isFalse();
    }

    private double consumed(String outcome) {
        var counter = registry.find(MessagingMetrics.CONSUMED)
                .tags("topic", Topics.PAYMENTS_RESULT, "outcome", outcome).counter();
        return counter == null ? 0 : counter.count();
    }
}
