package cz.demo.eda.order.event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class EventSerializationTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    @DisplayName("OrderCreated projde JSON round-tripem beze ztráty dat")
    void should_roundTripOrderCreated_whenSerialized() {
        OrderCreated event = OrderCreated.of("corr-1", "order-1", "cust-1", new BigDecimal("12.50"), "CZK");

        String json = mapper.writeValueAsString(event);
        OrderCreated restored = mapper.readValue(json, OrderCreated.class);

        assertThat(restored).isEqualTo(event);
        assertThat(json).contains("\"eventId\"", "\"timestamp\"", "\"correlationId\":\"corr-1\"");
    }

    @Test
    @DisplayName("OrderCommand se deserializuje na správný podtyp podle pole type")
    void should_deserializeSubtype_whenReadAsOrderCommand() {
        OrderCommand confirm = ConfirmOrder.of("c", "o-1", "p-1");
        OrderCommand cancel = CancelOrder.of("c", "o-2", "declined");

        String confirmJson = mapper.writeValueAsString(confirm);
        String cancelJson = mapper.writeValueAsString(cancel);

        assertThat(confirmJson).contains("\"type\":\"ConfirmOrder\"", "\"paymentId\":\"p-1\"");
        assertThat(cancelJson).contains("\"type\":\"CancelOrder\"", "\"reason\":\"declined\"");
        assertThat(mapper.readValue(confirmJson, OrderCommand.class)).isEqualTo(confirm);
        assertThat(mapper.readValue(cancelJson, OrderCommand.class)).isEqualTo(cancel);
    }

    @Test
    @DisplayName("Vyhodí výjimku když chybí orderId")
    void should_throw_whenOrderIdIsNull() {
        assertThatNullPointerException()
                .isThrownBy(() -> OrderCreated.of("c", null, "cust", BigDecimal.ONE, "CZK"));
    }

    @Test
    @DisplayName("Každý nový příkaz dostane unikátní eventId")
    void should_generateUniqueEventIds_whenCreatedTwice() {
        CancelOrder first = CancelOrder.of("c", "o", "r");
        CancelOrder second = CancelOrder.of("c", "o", "r");

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }

    @Test
    @DisplayName("Vyhodí výjimku když ConfirmOrder nemá orderId")
    void should_throw_whenCommandOrderIdIsNull() {
        assertThatNullPointerException().isThrownBy(() -> ConfirmOrder.of("c", null, "p-1"));
    }
}
