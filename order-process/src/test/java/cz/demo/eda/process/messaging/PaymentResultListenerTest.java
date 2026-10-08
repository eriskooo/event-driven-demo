package cz.demo.eda.process.messaging;

import cz.demo.eda.process.event.PaymentCompleted;
import cz.demo.eda.process.event.PaymentFailed;
import cz.demo.eda.process.process.ProcessGateway;
import cz.demo.eda.process.process.ProcessMessages;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultListenerTest {

    @Mock
    private ProcessGateway gateway;

    @Test
    @DisplayName("PaymentCompleted zkoreluje do procesu se stavem COMPLETED a paymentId")
    void should_correlateCompleted_whenPaymentCompleted() {
        PaymentCompleted event = new PaymentCompleted(UUID.randomUUID(), Instant.now(), "c", "o-1", "p-1", BigDecimal.TEN);
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(true);

        new PaymentResultListener(gateway).onPaymentResult(event);

        Map<String, Object> variables = captureVariables(event.eventId());
        assertThat(variables).containsEntry("paymentStatus", "COMPLETED").containsEntry("paymentId", "p-1")
                .containsEntry("failureReason", null);
    }

    @Test
    @DisplayName("PaymentFailed zkoreluje do procesu se stavem FAILED a důvodem")
    void should_correlateFailed_whenPaymentFailed() {
        PaymentFailed event = new PaymentFailed(UUID.randomUUID(), Instant.now(), "c", "o-1", "declined");
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(true);

        new PaymentResultListener(gateway).onPaymentResult(event);

        Map<String, Object> variables = captureVariables(event.eventId());
        assertThat(variables).containsEntry("paymentStatus", "FAILED").containsEntry("failureReason", "declined")
                .containsEntry("paymentId", null);
    }

    @Test
    @DisplayName("Duplicitní výsledek platby jen přeskočí")
    void should_notThrow_whenDuplicate() {
        PaymentFailed event = new PaymentFailed(UUID.randomUUID(), Instant.now(), "c", "o-1", "declined");
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(false);

        new PaymentResultListener(gateway).onPaymentResult(event);

        verify(gateway).publish(eq(ProcessMessages.PAYMENT_RESULT), eq("o-1"), eq(event.eventId()), anyMap());
    }

    @Test
    @DisplayName("Selhání brány propaguje, aby Kafka zprávu zopakovala a případně poslala do DLT")
    void should_propagate_whenGatewayFails() {
        PaymentFailed event = new PaymentFailed(UUID.randomUUID(), Instant.now(), "c", "o-1", "declined");
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenThrow(new IllegalStateException("zeebe down"));
        PaymentResultListener listener = new PaymentResultListener(gateway);

        assertThatThrownBy(() -> listener.onPaymentResult(event)).isInstanceOf(IllegalStateException.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> captureVariables(UUID eventId) {
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(gateway).publish(eq(ProcessMessages.PAYMENT_RESULT), eq("o-1"), eq(eventId), captor.capture());
        return captor.getValue();
    }
}
