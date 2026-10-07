package cz.demo.eda.payment.outbox;

import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Tracing;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Odesílá zprávy z outboxu do Kafky (at-least-once). Řádky se označí jako publikované až po potvrzení
 * brokeru; při chybě se transakce vrátí a dávka se pošle znovu – duplicity řeší inbox konzumentů.
 */
@Service
@Transactional
public class OutboxService implements DisposableBean {

    static final int BATCH_SIZE = 100;
    private static final long SEND_TIMEOUT_MS = 10_000;
    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

    private final OutboxRepository repository;
    private final KafkaOperations<String, String> kafka;
    private final MessagingMetrics metrics;
    private final Clock clock;

    /**
     * Payload je v outboxu už jako JSON, proto se posílá String (StringSerializer). Vlastní šablona
     * není Spring bean, jinak by Boot nevytvořil výchozí KafkaTemplate pro DLT recoverer.
     */
    @Autowired
    public OutboxService(OutboxRepository repository, ProducerFactory<Object, Object> producerFactory,
                         MessagingMetrics metrics, Clock clock) {
        this(repository, stringTemplate(producerFactory), metrics, clock);
    }

    OutboxService(OutboxRepository repository, KafkaOperations<String, String> kafka, MessagingMetrics metrics,
                  Clock clock) {
        this.repository = repository;
        this.kafka = kafka;
        this.metrics = metrics;
        this.clock = clock;
    }

    /**
     * Zamkne dávku nepublikovaných zpráv, odešle ji a označí jako publikovanou.
     *
     * @return počet odeslaných zpráv; {@link #BATCH_SIZE} znamená, že mohou čekat další
     */
    public int publishBatch() {
        List<OutboxEntry> batch = repository.lockUnpublished(Limit.of(BATCH_SIZE));
        if (batch.isEmpty()) {
            return 0;
        }
        // Odeslat vše najednou a pak čekat – rychlejší než čekat na každou zprávu zvlášť;
        // pořadí v rámci partition drží idempotentní producer.
        List<CompletableFuture<SendResult<String, String>>> sends = batch.stream().map(this::send).toList();
        for (int i = 0; i < batch.size(); i++) {
            await(sends.get(i));
            OutboxEntry entry = batch.get(i);
            // Dirty checking – UPDATE published_at proběhne při commitu transakce.
            entry.markPublished(clock.instant());
            onPublished(entry);
        }
        return batch.size();
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxEntry entry) {
        ProducerRecord<String, String> record = new ProducerRecord<>(entry.topic(), entry.messageKey(), entry.payload());
        entry.headers().forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
        return kafka.send(record);
    }

    private void onPublished(OutboxEntry entry) {
        metrics.produced(entry.topic());
        MDC.put(Tracing.CORRELATION_ID_MDC_KEY, entry.headers().get(Tracing.CORRELATION_ID_HEADER));
        try {
            log.info("Published event {} for order {} to {}", entry.eventId(), entry.messageKey(), entry.topic());
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
        ProducerFactory<String, String> factory = (ProducerFactory<String, String>) (ProducerFactory<?, ?>) producerFactory;
        return new KafkaTemplate<>(factory, Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    @Override
    public void destroy() throws Exception {
        if (kafka instanceof KafkaTemplate<?, ?> template && template.getProducerFactory() instanceof DisposableBean factory) {
            factory.destroy();
        }
    }

    /** Odeslání zprávy z outboxu selhalo; transakce se vrátí a dávka se zopakuje. */
    static class OutboxPublishException extends RuntimeException {
        OutboxPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
