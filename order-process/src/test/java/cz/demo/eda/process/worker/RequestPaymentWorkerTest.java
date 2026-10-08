package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ProcessPayment;
import cz.demo.eda.process.messaging.CommandPublishException;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.support.Topics;
import cz.demo.eda.process.support.Tracing;
import io.camunda.client.api.response.ActivatedJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestPaymentWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    private RequestPaymentWorker worker;

    @BeforeEach
    void setUp() {
        worker = new RequestPaymentWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC));
        when(job.getKey()).thenReturn(42L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", new BigDecimal("10.50"), "CZK", "corr-1", null, null, null));
    }

    @Test
    @DisplayName("Pošle ProcessPayment do payments.commands s eventId odvozeným z jobKey")
    void should_sendProcessPayment_whenJobActivated() {
        worker.requestPayment(job);

        ArgumentCaptor<Object> command = ArgumentCaptor.forClass(Object.class);
        verify(publisher).send(eq(Topics.PAYMENTS_COMMANDS), eq("o-1"), command.capture(), eq("corr-1"));
        assertThat(command.getValue()).isEqualTo(new ProcessPayment(CommandPublisher.commandId(42L), NOW, "corr-1",
                "o-1", new BigDecimal("10.50"), "CZK"));
    }

    @Test
    @DisplayName("correlationId je v MDC po dobu odesílání a po návratu se uklidí")
    void should_setMdcDuringSend_whenCorrelationIdPresent() {
        AtomicReference<String> seen = new AtomicReference<>();
        doAnswer(invocation -> {
            seen.set(MDC.get(Tracing.CORRELATION_ID_MDC_KEY));
            return null;
        }).when(publisher).send(anyString(), anyString(), any(), anyString());

        worker.requestPayment(job);

        assertThat(seen.get()).isEqualTo("corr-1");
        assertThat(MDC.get(Tracing.CORRELATION_ID_MDC_KEY)).isNull();
    }

    @Test
    @DisplayName("Selhání odeslání propaguje – job zůstane nedokončený a Zeebe sníží retries")
    void should_propagate_whenPublishFails() {
        doThrow(new CommandPublishException("payments.commands", new IllegalStateException("down")))
                .when(publisher).send(anyString(), anyString(), any(), anyString());

        assertThatThrownBy(() -> worker.requestPayment(job)).isInstanceOf(CommandPublishException.class);
    }
}
