package cz.demo.eda.order.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Výsledek platby. Oba podtypy sdílí jeden topic, proto nesou v JSON diskriminátor {@code type}
 * místo Spring type hlaviček – konzument tak nezávisí na Java třídách producenta.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PaymentCompleted.class, name = "PaymentCompleted"),
        @JsonSubTypes.Type(value = PaymentFailed.class, name = "PaymentFailed")
})
public sealed interface PaymentResult extends DomainEvent permits PaymentCompleted, PaymentFailed {
}
