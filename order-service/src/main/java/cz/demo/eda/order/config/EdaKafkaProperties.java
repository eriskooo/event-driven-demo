package cz.demo.eda.order.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Nastavení topiců a retry politiky konzumenta.
 *
 * @param topics počet partitions a replik vytvářených topiců
 * @param retry  exponenciální backoff před přesunem zprávy do DLT
 */
@ConfigurationProperties("eda.kafka")
public record EdaKafkaProperties(@DefaultValue Topics topics, @DefaultValue Retry retry) {

    /** Parametry vytvářených topiců. */
    public record Topics(@DefaultValue("3") int partitions, @DefaultValue("1") short replicas) {
    }

    /** Parametry exponenciálního backoffu. */
    public record Retry(
            @DefaultValue("3") int maxRetries,
            @DefaultValue("500ms") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("5s") Duration maxInterval) {
    }
}
