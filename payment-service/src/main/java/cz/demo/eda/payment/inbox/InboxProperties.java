package cz.demo.eda.payment.inbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Retry politika zpracování inboxu.
 *
 * @param maxAttempts     celkový počet pokusů (1 + retry), po jejich vyčerpání jde zpráva do DLT
 * @param initialBackoff  prodleva před prvním retry
 * @param multiplier      násobitel prodlevy pro každý další retry
 * @param maxBackoff      horní mez prodlevy
 * @param batchSize       kolik zpráv se zpracuje v jednom běhu processoru
 */
@ConfigurationProperties("eda.inbox")
public record InboxProperties(
        @DefaultValue("4") int maxAttempts,
        @DefaultValue("500ms") Duration initialBackoff,
        @DefaultValue("2.0") double multiplier,
        @DefaultValue("5s") Duration maxBackoff,
        @DefaultValue("50") int batchSize) {

    /** Vrátí prodlevu před dalším pokusem po {@code failedAttempts} neúspěšných pokusech. */
    public Duration backoffAfter(int failedAttempts) {
        var millis = initialBackoff.toMillis() * Math.pow(multiplier, Math.max(0, failedAttempts - 1));
        return Duration.ofMillis((long) Math.min(millis, maxBackoff.toMillis()));
    }
}
