package cz.demo.eda.payment;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

/** Pomocník pro integrační testy: čte a zapisuje topicy jako surový text, nezávisle na konfiguraci aplikace. */
public final class KafkaTestSupport {

    private KafkaTestSupport() {
    }

    /** Čte topic od začátku, dokud nenajde záznam splňující podmínku, nebo nevyprší timeout. */
    public static ConsumerRecord<String, String> awaitRecord(String bootstrapServers, String topic,
                                                             Predicate<ConsumerRecord<String, String>> match,
                                                             Duration timeout) {
        return readRecords(bootstrapServers, topic, match, timeout, true).stream().findFirst()
                .orElseThrow(() -> new AssertionError("No matching record in " + topic + " within " + timeout));
    }

    /** Přečte z topicu všechny záznamy splňující podmínku za danou dobu (pro ověření, že nepřišlo nic navíc). */
    public static List<ConsumerRecord<String, String>> collectRecords(String bootstrapServers, String topic,
                                                                      Predicate<ConsumerRecord<String, String>> match,
                                                                      Duration duration) {
        return readRecords(bootstrapServers, topic, match, duration, false);
    }

    /** Odešle surovou textovou zprávu – např. nevalidní JSON pro test DLT. */
    public static void sendRaw(String bootstrapServers, String topic, String key, String value) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send raw record to " + topic, e);
        }
    }

    /** Vrátí hodnotu hlavičky jako text, nebo null. */
    public static String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static List<ConsumerRecord<String, String>> readRecords(String bootstrapServers, String topic,
                                                                    Predicate<ConsumerRecord<String, String>> match,
                                                                    Duration duration, boolean stopOnFirst) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var found = new ArrayList<ConsumerRecord<String, String>>();
        var deadline = Instant.now().plus(duration);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            while (Instant.now().isBefore(deadline)) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (match.test(record)) {
                        found.add(record);
                    }
                }
                if (stopOnFirst && !found.isEmpty()) {
                    break;
                }
            }
        }
        return List.copyOf(found);
    }
}
