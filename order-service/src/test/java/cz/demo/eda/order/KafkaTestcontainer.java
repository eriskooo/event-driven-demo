package cz.demo.eda.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;

/** Kafka broker pro integrační testy – stejná verze jako v Kubernetes. */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestcontainer {

    /** Kontejner sdílený v rámci Spring test kontextu; @ServiceConnection nastaví bootstrap servery. */
    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:4.3.1");
    }
}
