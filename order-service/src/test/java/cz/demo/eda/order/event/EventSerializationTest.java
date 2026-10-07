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
    @DisplayName("PaymentResult se deserializuje na správný podtyp podle pole type")
    void should_deserializeSubtype_whenReadAsPaymentResult() {
        PaymentResult completed = PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.TEN);
        PaymentResult failed = PaymentFailed.of("c", "o-2", "declined");

        String completedJson = mapper.writeValueAsString(completed);
        String failedJson = mapper.writeValueAsString(failed);

        assertThat(completedJson).contains("\"type\":\"PaymentCompleted\"");
        assertThat(mapper.readValue(completedJson, PaymentResult.class)).isEqualTo(completed);
        assertThat(mapper.readValue(failedJson, PaymentResult.class)).isEqualTo(failed);
    }

    @Test
    @DisplayName("Vyhodí výjimku když chybí orderId")
    void should_throw_whenOrderIdIsNull() {
        assertThatNullPointerException()
                .isThrownBy(() -> OrderCreated.of("c", null, "cust", BigDecimal.ONE, "CZK"));
    }

    @Test
    @DisplayName("Každá nová událost dostane unikátní eventId")
    void should_generateUniqueEventIds_whenCreatedTwice() {
        PaymentFailed first = PaymentFailed.of("c", "o", "r");
        PaymentFailed second = PaymentFailed.of("c", "o", "r");

        assertThat(first.eventId()).isNotEqualTo(second.eventId());
    }
}
