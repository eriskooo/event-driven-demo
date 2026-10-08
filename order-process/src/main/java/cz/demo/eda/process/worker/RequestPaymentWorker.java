package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ProcessPayment;
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

/** Service task „Request payment": pošle payment-service příkaz k platbě. */
@Component
public class RequestPaymentWorker {

    private static final Logger log = LoggerFactory.getLogger(RequestPaymentWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    /** Vytvoří worker nad publisherem příkazů a hodinami (kvůli testovatelnosti času). */
    public RequestPaymentWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle ProcessPayment; job se dokončí automaticky po návratu (tedy po ack Kafky). */
    @JobWorker(type = ProcessMessages.JOB_REQUEST_PAYMENT)
    public void requestPayment(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        try (CorrelationScope ignored = CorrelationScope.open(variables.correlationId())) {
            ProcessPayment command = new ProcessPayment(CommandPublisher.commandId(job.getKey()), clock.instant(),
                    variables.correlationId(), variables.orderId(), variables.amount(), variables.currency());
            publisher.send(Topics.PAYMENTS_COMMANDS, variables.orderId(), command, variables.correlationId());
            log.info("ProcessPayment {} sent for order {}", command.eventId(), variables.orderId());
        }
    }
}
