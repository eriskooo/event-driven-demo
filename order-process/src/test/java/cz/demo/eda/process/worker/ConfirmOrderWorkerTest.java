package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ConfirmOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.api.response.ActivatedJob;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfirmOrderWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    @Test
    @DisplayName("Pošle ConfirmOrder do orders.commands s paymentId z procesu")
    void should_sendConfirmOrder_whenJobActivated() {
        when(job.getKey()).thenReturn(7L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", BigDecimal.TEN, "CZK", "corr-1", "COMPLETED", "p-1", null));

        new ConfirmOrderWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC)).confirmOrder(job);

        verify(publisher).send(Topics.ORDERS_COMMANDS, "o-1",
                new ConfirmOrder(CommandPublisher.commandId(7L), NOW, "corr-1", "o-1", "p-1"), "corr-1");
    }
}
