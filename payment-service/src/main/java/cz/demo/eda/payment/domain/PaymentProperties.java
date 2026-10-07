package cz.demo.eda.payment.domain;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;

/**
 * Nastavení simulace plateb.
 *
 * @param failureRate  pravděpodobnost (0–1) business zamítnutí platby → PaymentFailed
 * @param poisonAmount částka, která vždy vyvolá technickou chybu → retry a DLT
 */
@Validated
@ConfigurationProperties("payment")
public record PaymentProperties(
        @DecimalMin("0.0") @DecimalMax("1.0") @DefaultValue("0.2") double failureRate,
        @NotNull @DefaultValue("666") BigDecimal poisonAmount) {
}
