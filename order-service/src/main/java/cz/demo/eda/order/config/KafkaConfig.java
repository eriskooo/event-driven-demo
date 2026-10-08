package cz.demo.eda.order.config;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.MessagingMetrics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.LinkedHashMap;
import java.util.Map;

/** Topicy, retry s backoffem a dead letter topic pro konzumenta orders.commands. */
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    /**
     * Topicy, které služba produkuje nebo konzumuje, plus DLT svého konzumenta.
     * KafkaAdmin je vytvoří při startu; existující topicy nechá být.
     */
    @Bean
    KafkaAdmin.NewTopics edaTopics(EdaKafkaProperties properties) {
        EdaKafkaProperties.Topics settings = properties.topics();
        return new KafkaAdmin.NewTopics(
                topic(Topics.ORDERS_CREATED, settings),
                topic(Topics.ORDERS_COMMANDS, settings),
                // DLT má stejný počet partitions, aby recoverer mohl zachovat číslo partition.
                topic(Topics.dltOf(Topics.ORDERS_COMMANDS), settings));
    }

    /** Error handler: retry s exponenciálním backoffem, poté publikace do &lt;topic&gt;.DLT. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate,
                                          ProducerFactory<Object, Object> producerFactory,
                                          EdaKafkaProperties properties, MessagingMetrics metrics) {
        metrics.deadLetterCounter(Topics.dltOf(Topics.ORDERS_COMMANDS));
        DeadLetterPublishingRecoverer recoverer = deadLetterRecoverer(kafkaTemplate, producerFactory);
        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            recoverer.accept(record, ex);
            metrics.deadLettered(Topics.dltOf(record.topic()));
            log.error("Record {}-{}@{} moved to {} after retries", record.topic(), record.partition(),
                    record.offset(), Topics.dltOf(record.topic()), ex);
        }, backOff(properties.retry()));
        handler.setRetryListeners((record, ex, attempt) -> log.warn("Delivery attempt {} of record {}-{}@{} failed: {}",
                attempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));
        return handler;
    }

    private static DeadLetterPublishingRecoverer deadLetterRecoverer(KafkaTemplate<Object, Object> jsonTemplate,
                                                                     ProducerFactory<Object, Object> producerFactory) {
        // Nedeserializovatelná zpráva má jako hodnotu původní byte[] – ten musí jít do DLT beze změny,
        // JSON serializer by ho zakódoval do base64.
        KafkaTemplate<Object, Object> bytesTemplate = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, jsonTemplate);
        return new DeadLetterPublishingRecoverer(templates, KafkaConfig::dltPartition);
    }

    private static TopicPartition dltPartition(ConsumerRecord<?, ?> record, Exception ex) {
        return new TopicPartition(Topics.dltOf(record.topic()), record.partition());
    }

    private static ExponentialBackOffWithMaxRetries backOff(EdaKafkaProperties.Retry retry) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        return backOff;
    }

    private static NewTopic topic(String name, EdaKafkaProperties.Topics settings) {
        return TopicBuilder.name(name).partitions(settings.partitions()).replicas(settings.replicas()).build();
    }
}
