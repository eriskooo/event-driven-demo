package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.CancelOrder;
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
class CancelOrderWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    @Test
    @DisplayName("Pošle CancelOrder do orders.commands s důvodem zamítnutí")
    void should_sendCancelOrder_whenJobActivated() {
        when(job.getKey()).thenReturn(8L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", BigDecimal.TEN, "CZK", null, "FAILED", null, "declined"));

        new CancelOrderWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC)).cancelOrder(job);

        verify(publisher).send(Topics.ORDERS_COMMANDS, "o-1",
                new CancelOrder(CommandPublisher.commandId(8L), NOW, null, "o-1", "declined"), null);
    }
}
