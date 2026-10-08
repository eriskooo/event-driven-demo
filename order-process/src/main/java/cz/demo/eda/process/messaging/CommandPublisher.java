package cz.demo.eda.process.messaging;

import cz.demo.eda.process.support.Tracing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Odesílá příkazy do Kafky synchronně – job worker smí job dokončit až po potvrzení brokeru,
 * jinak by se při pádu příkaz ztratil. Outbox tu není potřeba: opakování zajistí Zeebe (job se rozdá znovu).
 */
@Component
public class CommandPublisher {

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    /** Vytvoří publisher nad KafkaTemplate. */
    public CommandPublisher(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Deterministické ID příkazu z klíče jobu: opakovaný job pošle příkaz se stejným eventId
     * a inbox příjemce ho zahodí jako duplicitu.
     */
    public static UUID commandId(long jobKey) {
        return UUID.nameUUIDFromBytes(("job-" + jobKey).getBytes(StandardCharsets.UTF_8));
    }

    /** Odešle příkaz a počká na potvrzení brokeru; při chybě vyhodí {@link CommandPublishException}. */
    public void send(String topic, String key, Object command, String correlationId) {
        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, key, command);
        if (correlationId != null) {
            record.headers().add(Tracing.CORRELATION_ID_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandPublishException(topic, e);
        } catch (ExecutionException | TimeoutException e) {
            // Future zůstává běžet; pozdní doručení je neškodné – retry použije stejné commandId a inbox příjemce deduplikuje.
            throw new CommandPublishException(topic, e);
        }
    }
}
