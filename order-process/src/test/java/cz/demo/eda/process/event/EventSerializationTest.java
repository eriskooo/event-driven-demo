package cz.demo.eda.process.event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class EventSerializationTest {

    private static final UUID ID = UUID.fromString("5b7c0000-0000-4000-8000-000000000001");
    private static final Instant TS = Instant.parse("2026-10-08T10:00:00Z");

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    @DisplayName("Přečte OrderCreated v JSON formátu, který publikuje order-service")
    void should_readOrderCreated_whenJsonFromOrderService() {
        String json = """
                {"eventId":"5b7c0000-0000-4000-8000-000000000001","timestamp":"2026-10-08T10:00:00Z",
                 "correlationId":"demo-1","orderId":"o-1","customerId":"c1","amount":10.5,"currency":"CZK"}""";

        OrderCreated event = mapper.readValue(json, OrderCreated.class);

        assertThat(event).isEqualTo(new OrderCreated(ID, TS, "demo-1", "o-1", "c1", new BigDecimal("10.5"), "CZK"));
    }

    @Test
    @DisplayName("PaymentResult se deserializuje na podtyp podle pole type")
    void should_deserializeSubtype_whenReadAsPaymentResult() {
        String completed = """
                {"type":"PaymentCompleted","eventId":"5b7c0000-0000-4000-8000-000000000001",
                 "timestamp":"2026-10-08T10:00:00Z","correlationId":"c","orderId":"o-1","paymentId":"p-1","amount":10}""";
        String failed = """
                {"type":"PaymentFailed","eventId":"5b7c0000-0000-4000-8000-000000000001",
                 "timestamp":"2026-10-08T10:00:00Z","correlationId":"c","orderId":"o-1","reason":"declined"}""";

        assertThat(mapper.readValue(completed, PaymentResult.class))
                .isEqualTo(new PaymentCompleted(ID, TS, "c", "o-1", "p-1", BigDecimal.TEN));
        assertThat(mapper.readValue(failed, PaymentResult.class))
                .isEqualTo(new PaymentFailed(ID, TS, "c", "o-1", "declined"));
    }

    @Test
    @DisplayName("ProcessPayment se serializuje bez pole type a se všemi poli kontraktu")
    void should_writeProcessPaymentContract_whenSerialized() {
        String json = mapper.writeValueAsString(new ProcessPayment(ID, TS, "c", "o-1", BigDecimal.TEN, "CZK"));

        assertThat(json).contains("\"eventId\":\"" + ID + "\"", "\"orderId\":\"o-1\"", "\"amount\":10",
                "\"currency\":\"CZK\"", "\"correlationId\":\"c\"").doesNotContain("\"type\"");
    }

    @Test
    @DisplayName("Příkazy pro objednávku nesou diskriminátor type")
    void should_writeTypeDiscriminator_whenOrderCommandSerialized() {
        OrderCommand confirm = new ConfirmOrder(ID, TS, "c", "o-1", "p-1");
        OrderCommand cancel = new CancelOrder(ID, TS, "c", "o-1", "declined");

        assertThat(mapper.writeValueAsString(confirm)).contains("\"type\":\"ConfirmOrder\"", "\"paymentId\":\"p-1\"");
        assertThat(mapper.writeValueAsString(cancel)).contains("\"type\":\"CancelOrder\"", "\"reason\":\"declined\"");
    }

    @Test
    @DisplayName("Vyhodí výjimku když příkaz nemá orderId")
    void should_throw_whenCommandOrderIdIsNull() {
        assertThatNullPointerException().isThrownBy(() -> new ProcessPayment(ID, TS, "c", null, BigDecimal.ONE, "CZK"));
    }
}
