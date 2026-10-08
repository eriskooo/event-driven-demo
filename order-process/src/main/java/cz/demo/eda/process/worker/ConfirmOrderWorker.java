package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ConfirmOrder;
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

/** Service task „Confirm order": pošle order-service příkaz k potvrzení objednávky. */
@Component
public class ConfirmOrderWorker {

    private static final Logger log = LoggerFactory.getLogger(ConfirmOrderWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    /** Vytvoří worker nad publisherem příkazů a hodinami (kvůli testovatelnosti času). */
    public ConfirmOrderWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle ConfirmOrder s paymentId z výsledku platby. */
    @JobWorker(type = ProcessMessages.JOB_CONFIRM_ORDER)
    public void confirmOrder(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        try (CorrelationScope ignored = CorrelationScope.open(variables.correlationId())) {
            ConfirmOrder command = new ConfirmOrder(CommandPublisher.commandId(job.getKey()), clock.instant(),
                    variables.correlationId(), variables.orderId(), variables.paymentId());
            publisher.send(Topics.ORDERS_COMMANDS, variables.orderId(), command, variables.correlationId());
            log.info("ConfirmOrder {} sent for order {}", command.eventId(), variables.orderId());
        }
    }
}
