package cz.demo.eda.process.process;

import cz.demo.eda.process.config.ProcessProperties;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.ClientStatusException;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Jediné místo, kde služba posílá zprávy do Zeebe; idempotenci zajišťuje messageId = eventId z Kafky. */
@Component
public class ProcessGateway {

    private static final Logger log = LoggerFactory.getLogger(ProcessGateway.class);

    private final CamundaClient client;
    private final ProcessProperties properties;

    /** Vytvoří bránu nad Camunda klientem; TTL zpráv se bere z konfigurace procesu. */
    public ProcessGateway(CamundaClient client, ProcessProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    /**
     * Publikuje zprávu do Zeebe a počká na potvrzení. Zeebe ji zkoreluje s instancí čekající na stejný
     * název a correlation key, nebo ji po dobu TTL podrží.
     *
     * @return false, pokud zpráva se stejným messageId už v Zeebe je (duplicitní doručení)
     */
    public boolean publish(String messageName, String correlationKey, UUID messageId, Map<String, Object> variables) {
        Objects.requireNonNull(messageId, "messageId");
        try {
            client.newPublishMessageCommand()
                    .messageName(messageName)
                    .correlationKey(correlationKey)
                    .messageId(messageId.toString())
                    .timeToLive(properties.messageTtl())
                    .variables(variables)
                    .send()
                    .join();
            log.info("Message {} published for {} (messageId {})", messageName, correlationKey, messageId);
            return true;
        } catch (ClientStatusException e) {
            if (e.getStatusCode() != Status.Code.ALREADY_EXISTS) {
                throw e;
            }
            log.info("Duplicate message {} for {} (messageId {}) skipped", messageName, correlationKey, messageId);
            return false;
        }
    }
}
