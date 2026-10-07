package cz.demo.eda.payment.config;

import cz.demo.eda.payment.support.ProcessedEventStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Random;
import java.util.random.RandomGenerator;

/** Obecné beany aplikace. */
@Configuration(proxyBeanMethods = false)
public class AppConfig {

    /**
     * Zdroj náhody pro simulaci; jako bean kvůli deterministickým testům.
     * java.util.Random místo RandomGenerator.getDefault() – ten hledá algoritmus přes ServiceLoader
     * a v JRE kontejneru selhal; Random je navíc thread-safe pro souběžné listenery.
     */
    @Bean
    RandomGenerator randomGenerator() {
        return new Random();
    }

    /** Paměť zpracovaných eventId pro idempotentní listener. */
    @Bean
    ProcessedEventStore processedEventStore() {
        return new ProcessedEventStore();
    }
}
