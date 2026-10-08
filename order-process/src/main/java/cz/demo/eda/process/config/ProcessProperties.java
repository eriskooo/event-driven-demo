package cz.demo.eda.process.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Nastavení napojení na proces.
 *
 * @param messageTtl jak dlouho Zeebe drží publikovanou zprávu, než ji někdo zkoreluje. Musí být delší než
 *                   timeout jobu (výchozí 5 min) plus doba řešení incidentu – jinak výsledek platby, který
 *                   dorazil, dokud instance ještě stála na request_payment, vyprší a instance čeká donekonečna.
 *                   Zároveň je to okno deduplikace OrderCreated podle messageId.
 */
@ConfigurationProperties("eda.process")
public record ProcessProperties(@DefaultValue("1h") Duration messageTtl) {

    /** Ověří, že TTL zpráv je kladné. */
    public ProcessProperties {
        if (messageTtl == null || messageTtl.isNegative() || messageTtl.isZero()) {
            throw new IllegalArgumentException("eda.process.message-ttl must be positive");
        }
    }
}
