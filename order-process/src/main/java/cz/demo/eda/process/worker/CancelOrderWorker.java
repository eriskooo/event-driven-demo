package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.CancelOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

/** Service task „Cancel order": pošle order-service příkaz ke zrušení objednávky. */
@Component
public class CancelOrderWorker {

    private static final Logger log = LoggerFactory.getLogger(CancelOrderWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    /** Vytvoří worker nad publisherem příkazů a hodinami (kvůli testovatelnosti času). */
    public CancelOrderWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle CancelOrder s důvodem zamítnutí platby. */
    @JobWorker(type = ProcessMessages.JOB_CANCEL_ORDER)
    public void cancelOrder(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        try (CorrelationScope ignored = CorrelationScope.open(variables.correlationId())) {
            CancelOrder command = new CancelOrder(CommandPublisher.commandId(job.getKey()), clock.instant(),
                    variables.correlationId(), variables.orderId(), variables.failureReason());
            publisher.send(Topics.ORDERS_COMMANDS, variables.orderId(), command, variables.correlationId());
            log.info("CancelOrder {} sent for order {}", command.eventId(), variables.orderId());
        }
    }
}
