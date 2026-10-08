package cz.demo.eda.process.messaging;

import cz.demo.eda.process.event.OrderCreated;
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderCreatedListenerTest {

    private static final OrderCreated EVENT = new OrderCreated(UUID.randomUUID(), Instant.now(), "corr-1", "o-1",
            "c-1", new BigDecimal("10.50"), "CZK");

    @Mock
    private ProcessGateway gateway;

    @Test
    @DisplayName("Spustí proces zprávou OrderCreated s messageId = eventId a proměnnými objednávky")
    @SuppressWarnings("unchecked")
    void should_startProcess_whenOrderCreated() {
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(true);

        new OrderCreatedListener(gateway).onOrderCreated(EVENT);

        ArgumentCaptor<Map<String, Object>> variables = ArgumentCaptor.forClass(Map.class);
        verify(gateway).publish(eq(ProcessMessages.ORDER_CREATED), eq("o-1"), eq(EVENT.eventId()), variables.capture());
        assertThat(variables.getValue()).containsEntry("orderId", "o-1")
                .containsEntry("amount", new BigDecimal("10.50"))
                .containsEntry("currency", "CZK")
                .containsEntry("correlationId", "corr-1");
    }

    @Test
    @DisplayName("Duplicitní OrderCreated nevyhodí výjimku – druhá instance procesu nevznikne")
    void should_notThrow_whenDuplicate() {
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(false);

        new OrderCreatedListener(gateway).onOrderCreated(EVENT);

        verify(gateway).publish(eq(ProcessMessages.ORDER_CREATED), eq("o-1"), eq(EVENT.eventId()), anyMap());
    }

    @Test
    @DisplayName("Chybějící correlationId předá jako null proměnnou místo pádu")
    @SuppressWarnings("unchecked")
    void should_passNullCorrelationId_whenMissing() {
        OrderCreated event = new OrderCreated(UUID.randomUUID(), Instant.now(), null, "o-2", "c", BigDecimal.ONE, "CZK");
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenReturn(true);

        new OrderCreatedListener(gateway).onOrderCreated(event);

        ArgumentCaptor<Map<String, Object>> variables = ArgumentCaptor.forClass(Map.class);
        verify(gateway).publish(anyString(), anyString(), any(), variables.capture());
        assertThat(variables.getValue()).containsEntry("correlationId", null);
    }

    @Test
    @DisplayName("Chybu Zeebe propaguje, aby ji Kafka error handler zopakoval")
    void should_propagate_whenGatewayFails() {
        when(gateway.publish(anyString(), anyString(), any(), anyMap())).thenThrow(new IllegalStateException("down"));

        assertThatIllegalStateException().isThrownBy(() -> new OrderCreatedListener(gateway).onOrderCreated(EVENT));
    }
}
