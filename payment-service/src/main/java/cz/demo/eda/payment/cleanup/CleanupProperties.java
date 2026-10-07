package cz.demo.eda.payment.cleanup;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Retence dokončených zpráv v inboxu a outboxu.
 *
 * @param retentionDays zprávy dokončené před více než tolika dny se smažou; po smazání už inbox
 *                      nerozpozná duplicitu takto staré zprávy
 * @param batchSize     kolik řádků smazat jedním příkazem (krátké zámky, malé transakce)
 * @param cron          kdy se úklid spouští (Spring cron, výchozí denně ve 03:00)
 */
@Validated
@ConfigurationProperties("eda.cleanup")
public record CleanupProperties(
        @Min(1) @DefaultValue("7") int retentionDays,
        @Min(1) @DefaultValue("1000") int batchSize,
        @DefaultValue("0 0 3 * * *") String cron) {
}
