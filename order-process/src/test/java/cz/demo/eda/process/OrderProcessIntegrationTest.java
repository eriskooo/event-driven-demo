package cz.demo.eda.process;

import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import cz.demo.eda.process.support.Tracing;
import io.camunda.process.test.api.CamundaAssert;
import io.camunda.process.test.api.CamundaSpringProcessTest;
import io.camunda.process.test.api.assertions.ProcessInstanceSelectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Celý tok přes skutečnou Kafku a Zeebe: OrderCreated → ProcessPayment → PaymentResult → Confirm/CancelOrder.
 * Zeebe běží v Testcontaineru spravovaném Camunda Process Test, workery a listenery jsou ty produkční.
 */
@SpringBootTest(properties = {
        "eda.kafka.retry.initial-interval=50ms",
        "eda.kafka.retry.max-interval=100ms"})
@CamundaSpringProcessTest
@Import(KafkaTestcontainer.class)
class OrderProcessIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private JsonMapper jsonMapper;

    @BeforeEach
    void setUp() {
        // Start Zeebe workeru a první poll Kafky trvají déle než výchozích 10 s.
        CamundaAssert.setAssertionTimeout(TIMEOUT);
    }

    @Test
    @DisplayName("Zaplacená objednávka projde procesem až do Confirm order a order-service dostane ConfirmOrder")
    void should_confirmOrder_whenPaymentCompleted() throws Exception {
        String orderId = newOrderId();

        sendOrderCreated(orderId, UUID.randomUUID());
        JsonNode payment = awaitCommand(Topics.PAYMENTS_COMMANDS, orderId);
        assertThat(payment.get("amount").decimalValue()).isEqualByComparingTo("10.50");
        assertThat(payment.get("correlationId").asString()).isEqualTo("it-" + orderId);
        sendPaymentResult(orderId, UUID.randomUUID(), "PaymentCompleted", "\"paymentId\":\"p-1\"");

        JsonNode confirm = awaitCommand(Topics.ORDERS_COMMANDS, orderId);
        assertThat(confirm.get("type").asString()).isEqualTo("ConfirmOrder");
        assertThat(confirm.get("paymentId").asString()).isEqualTo("p-1");
        CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId(ProcessMessages.PROCESS_ID))
                .isCompleted()
                .hasCompletedElements("request_payment", "payment_result", "confirm_order", "order_confirmed")
                .hasVariable("paymentStatus", "COMPLETED");
    }

    @Test
    @DisplayName("Zamítnutá platba vede na Cancel order s důvodem zamítnutí")
    void should_cancelOrder_whenPaymentFailed() throws Exception {
        String orderId = newOrderId();

        sendOrderCreated(orderId, UUID.randomUUID());
        awaitCommand(Topics.PAYMENTS_COMMANDS, orderId);
        sendPaymentResult(orderId, UUID.randomUUID(), "PaymentFailed", "\"reason\":\"declined\"");

        JsonNode cancel = awaitCommand(Topics.ORDERS_COMMANDS, orderId);
        assertThat(cancel.get("type").asString()).isEqualTo("CancelOrder");
        assertThat(cancel.get("reason").asString()).isEqualTo("declined");
        CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId(ProcessMessages.PROCESS_ID))
                .isCompleted()
                .hasCompletedElements("cancel_order", "order_cancelled");
    }

    @Test
    @DisplayName("Proces po odeslání příkazu k platbě čeká na zprávu PaymentResult")
    void should_waitForPaymentResult_whenPaymentRequested() throws Exception {
        String orderId = newOrderId();

        sendOrderCreated(orderId, UUID.randomUUID());
        awaitCommand(Topics.PAYMENTS_COMMANDS, orderId);

        CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId(ProcessMessages.PROCESS_ID))
                .isActive()
                .isWaitingForMessage(ProcessMessages.PAYMENT_RESULT);
    }

    @Test
    @DisplayName("Duplicitní OrderCreated (stejné eventId) spustí jen jednu instanci – odejde jen jeden ProcessPayment")
    void should_startSingleInstance_whenOrderCreatedDeliveredTwice() throws Exception {
        String orderId = newOrderId();
        UUID eventId = UUID.randomUUID();

        sendOrderCreated(orderId, eventId);
        sendOrderCreated(orderId, eventId);

        awaitCommand(Topics.PAYMENTS_COMMANDS, orderId);
        assertThat(collect(Topics.PAYMENTS_COMMANDS, orderId, Duration.ofSeconds(5))).hasSize(1);
        assertThat(collect(Topics.dltOf(Topics.ORDERS_CREATED), orderId, Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    @DisplayName("Výsledek platby, který předběhne proces, Zeebe podrží a po dojetí na čekání ho zkoreluje")
    void should_confirmOrder_whenPaymentResultArrivesBeforeProcessWaits() throws Exception {
        String orderId = newOrderId();

        sendPaymentResult(orderId, UUID.randomUUID(), "PaymentCompleted", "\"paymentId\":\"p-1\"");
        sendOrderCreated(orderId, UUID.randomUUID());

        JsonNode confirm = awaitCommand(Topics.ORDERS_COMMANDS, orderId);
        assertThat(confirm.get("type").asString()).isEqualTo("ConfirmOrder");
        assertThat(confirm.get("paymentId").asString()).isEqualTo("p-1");
        CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId(ProcessMessages.PROCESS_ID))
                .isCompleted();
    }

    @Test
    @DisplayName("Duplicitní PaymentResult se zkoreluje jen jednou – jeden ConfirmOrder a nic v DLT")
    void should_confirmOnce_whenPaymentResultDeliveredTwice() throws Exception {
        String orderId = newOrderId();
        UUID resultId = UUID.randomUUID();

        sendOrderCreated(orderId, UUID.randomUUID());
        awaitCommand(Topics.PAYMENTS_COMMANDS, orderId);
        sendPaymentResult(orderId, resultId, "PaymentCompleted", "\"paymentId\":\"p-1\"");
        sendPaymentResult(orderId, resultId, "PaymentCompleted", "\"paymentId\":\"p-1\"");

        awaitCommand(Topics.ORDERS_COMMANDS, orderId);
        assertThat(collect(Topics.ORDERS_COMMANDS, orderId, Duration.ofSeconds(5))).hasSize(1);
        assertThat(collect(Topics.dltOf(Topics.PAYMENTS_RESULT), orderId, Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    @DisplayName("PaymentResult pro objednávku bez čekající instance se uloží s TTL a nekončí v DLT")
    void should_notDeadLetter_whenPaymentResultForUnknownOrder() {
        String orderId = newOrderId();

        sendPaymentResult(orderId, UUID.randomUUID(), "PaymentFailed", "\"reason\":\"late\"");

        assertThat(collect(Topics.dltOf(Topics.PAYMENTS_RESULT), orderId, Duration.ofSeconds(5))).isEmpty();
        assertThat(collect(Topics.ORDERS_COMMANDS, orderId, Duration.ofSeconds(1))).isEmpty();
    }

    @Test
    @DisplayName("Nečitelná zpráva v orders.created skončí v DLT")
    void should_moveToDlt_whenOrderCreatedUnreadable() {
        KafkaTestSupport.sendRaw(kafka.getBootstrapServers(), Topics.ORDERS_CREATED, "bad-1", "not json");

        ConsumerRecord<String, String> dead = KafkaTestSupport.awaitRecord(kafka.getBootstrapServers(),
                Topics.dltOf(Topics.ORDERS_CREATED), r -> "bad-1".equals(r.key()), TIMEOUT);
        assertThat(dead.value()).isEqualTo("not json");
    }

    private static String newOrderId() {
        return "o-" + UUID.randomUUID();
    }

    private void sendOrderCreated(String orderId, UUID eventId) {
        KafkaTestSupport.sendRaw(kafka.getBootstrapServers(), Topics.ORDERS_CREATED, orderId, """
                {"eventId":"%s","timestamp":"2026-10-08T10:00:00Z","correlationId":"it-%s","orderId":"%s",
                 "customerId":"c1","amount":10.50,"currency":"CZK"}""".formatted(eventId, orderId, orderId));
    }

    private void sendPaymentResult(String orderId, UUID eventId, String type, String extraField) {
        KafkaTestSupport.sendRaw(kafka.getBootstrapServers(), Topics.PAYMENTS_RESULT, orderId, """
                {"type":"%s","eventId":"%s","timestamp":"2026-10-08T10:00:01Z","correlationId":"it-%s",
                 "orderId":"%s",%s}""".formatted(type, eventId, orderId, orderId, extraField));
    }

    private JsonNode awaitCommand(String topic, String orderId) throws Exception {
        ConsumerRecord<String, String> record = KafkaTestSupport.awaitRecord(kafka.getBootstrapServers(), topic,
                r -> orderId.equals(r.key()), TIMEOUT);
        assertThat(KafkaTestSupport.header(record, Tracing.CORRELATION_ID_HEADER)).isEqualTo("it-" + orderId);
        return jsonMapper.readTree(record.value());
    }

    private List<ConsumerRecord<String, String>> collect(String topic, String orderId, Duration duration) {
        return KafkaTestSupport.collectRecords(kafka.getBootstrapServers(), topic, r -> orderId.equals(r.key()), duration);
    }
}
