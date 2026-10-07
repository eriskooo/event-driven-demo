package cz.demo.eda.order.outbox;

import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Tracing;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionOperations;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Periodicky odesílá nepublikované zprávy z outboxu do Kafky (at-least-once).
 * Řádky se označí jako publikované až po potvrzení brokeru; při chybě transakce rollbackne
 * a dávka se pošle znovu – případné duplicity řeší idempotentní konzumenti.
 */
@Component
public class OutboxRelay implements DisposableBean {

    static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_MS = 10_000;
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaOperations<String, String> kafka;
    private final TransactionOperations transactions;
    private final MessagingMetrics metrics;

    /**
     * Payload je v outboxu už jako JSON, proto relay posílá String (StringSerializer). Vlastní šablona
     * není Spring bean, jinak by Boot nevytvořil výchozí KafkaTemplate pro ostatní producenty.
     */
    @Autowired
    public OutboxRelay(OutboxRepository repository, ProducerFactory<Object, Object> producerFactory,
                       TransactionOperations transactions, MessagingMetrics metrics) {
        this(repository, stringTemplate(producerFactory), transactions, metrics);
    }

    OutboxRelay(OutboxRepository repository, KafkaOperations<String, String> kafka,
                TransactionOperations transactions, MessagingMetrics metrics) {
        this.repository = repository;
        this.kafka = kafka;
        this.transactions = transactions;
        this.metrics = metrics;
    }

    /** Odešle všechny čekající zprávy po dávkách; chyba se zaloguje a zkusí se znovu při dalším běhu. */
    @Scheduled(fixedDelayString = "${eda.outbox.poll-interval-ms:500}")
    public void publishPending() {
        try {
            int sent;
            do {
                sent = transactions.execute(status -> publishBatch());
            } while (sent == BATCH_SIZE);
        } catch (RuntimeException e) {
            log.error("Outbox relay failed, pending messages will be retried: {}", e.getMessage(), e);
        }
    }

    /** Odešle jednu dávku a vrátí její velikost. Musí běžet v transakci (zámky řádků). */
    int publishBatch() {
        var batch = repository.lockUnpublished(BATCH_SIZE);
        if (batch.isEmpty()) {
            return 0;
        }
        // Odeslat vše najednou a pak čekat – rychlejší než čekat na každou zprávu zvlášť;
        // pořadí v rámci partition drží idempotentní producer.
        var sends = batch.stream().map(this::send).toList();
        for (int i = 0; i < batch.size(); i++) {
            await(sends.get(i));
            onPublished(batch.get(i));
        }
        repository.markPublished(batch.stream().map(OutboxMessage::id).toList());
        return batch.size();
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxMessage message) {
        var record = new ProducerRecord<>(message.topic(), message.key(), message.payload());
        message.headers().forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
        return kafka.send(record);
    }

    private void onPublished(OutboxMessage message) {
        metrics.produced(message.topic());
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, message.headers().get(Tracing.CORRELATION_ID_HEADER));
        try {
            log.info("Published event {} for order {} to {}", message.eventId(), message.key(), message.topic());
        } finally {
            MDC.remove(Tracing.CORRELATION_ID_MDC_KEY);
        }
    }

    private static void await(CompletableFuture<?> future) {
        try {
            future.get(SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException("Interrupted while publishing outbox message", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OutboxPublishException("Failed to publish outbox message", e);
        }
    }

    // Přetypování je bezpečné: klíč je String už v základní konfiguraci a value serializer přepisujeme na String.
    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> stringTemplate(ProducerFactory<Object, Object> producerFactory) {
        var factory = (ProducerFactory<String, String>) (ProducerFactory<?, ?>) producerFactory;
        return new KafkaTemplate<>(factory, Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    @Override
    public void destroy() throws Exception {
        if (kafka instanceof KafkaTemplate<?, ?> template && template.getProducerFactory() instanceof DisposableBean factory) {
            factory.destroy();
        }
    }

    /** Odeslání zprávy z outboxu selhalo; transakce se rollbackne a dávka se zopakuje. */
    static class OutboxPublishException extends RuntimeException {
        OutboxPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
