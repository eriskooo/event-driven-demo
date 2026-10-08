package cz.demo.eda.order.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Příkaz od orchestrátoru order-process. Oba podtypy sdílí jeden topic, proto nesou v JSON
 * diskriminátor {@code type} místo Spring type hlaviček.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConfirmOrder.class, name = "ConfirmOrder"),
        @JsonSubTypes.Type(value = CancelOrder.class, name = "CancelOrder")
})
public sealed interface OrderCommand extends DomainEvent permits ConfirmOrder, CancelOrder {
}
