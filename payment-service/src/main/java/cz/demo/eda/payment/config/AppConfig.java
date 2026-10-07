package cz.demo.eda.payment.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.Random;
import java.util.random.RandomGenerator;

/** Obecné beany aplikace. */
@Configuration(proxyBeanMethods = false)
public class AppConfig {

    /**
     * Zdroj náhody pro simulaci; jako bean kvůli deterministickým testům.
     * java.util.Random místo RandomGenerator.getDefault() – ten hledá algoritmus přes ServiceLoader
     * a v JRE kontejneru selhal; Random je navíc thread-safe.
     */
    @Bean
    RandomGenerator randomGenerator() {
        return new Random();
    }

    /** Systémové hodiny v UTC; jako bean kvůli deterministickým testům. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
