package cz.demo.eda.process.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Příkaz pro order-service. Oba podtypy sdílí jeden topic, proto nesou v JSON diskriminátor {@code type}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConfirmOrder.class, name = "ConfirmOrder"),
        @JsonSubTypes.Type(value = CancelOrder.class, name = "CancelOrder")
})
public sealed interface OrderCommand extends DomainEvent permits ConfirmOrder, CancelOrder {
}
