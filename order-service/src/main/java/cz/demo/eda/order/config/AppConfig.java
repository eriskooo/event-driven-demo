package cz.demo.eda.order.config;

import cz.demo.eda.order.support.ProcessedEventStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** Obecné beany aplikace. */
@Configuration(proxyBeanMethods = false)
public class AppConfig {

    /** Systémové hodiny v UTC; jako bean kvůli deterministickým testům. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Paměť zpracovaných eventId pro idempotentní listener. */
    @Bean
    ProcessedEventStore processedEventStore() {
        return new ProcessedEventStore();
    }
}
