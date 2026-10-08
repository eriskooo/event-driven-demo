### Task 7: BPMN model a end-to-end test procesu (Camunda Process Test + Kafka)

**Files:**
- Create: `order-process/src/main/resources/bpmn/order-fulfillment.bpmn`
- Create: `order-process/src/test/java/cz/demo/eda/process/KafkaTestcontainer.java` (kopie z order-service, balíček `cz.demo.eda.process`)
- Create: `order-process/src/test/java/cz/demo/eda/process/KafkaTestSupport.java` (kopie z order-service, balíček `cz.demo.eda.process`)
- Test: `order-process/src/test/java/cz/demo/eda/process/OrderProcessIntegrationTest.java`

**Interfaces:**
- Consumes: všechny třídy z Task 3–6; ID elementů v BPMN musí odpovídat `ProcessMessages` (job typy, názvy zpráv).
- Produces: ID elementů BPMN `order_created`, `request_payment`, `payment_result`, `payment_completed_gateway`, `confirm_order`, `cancel_order`, `order_confirmed`, `order_cancelled` (používá je test a README).

- [ ] **Step 1: Failing integrační test**

`OrderProcessIntegrationTest.java`:

```java
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
    @DisplayName("Nečitelná zpráva v orders.created skončí v DLT a proces nespustí")
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
```

> Poznámky pro implementátora:
> - `byProcessId` vybírá instance procesu; Camunda Process Test runtime mezi testy promaže data, takže v každém testu je vidět jen jeho instance. Pokud by se v 8.10.2 instance mezi testy přenášely (asserty selhávají na „more than one instance“), přidej nad třídu `@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)`.
> - `CamundaAssert` je v `io.camunda.process.test.api`, selektory v `io.camunda.process.test.api.assertions`; pokud IDE hlásí jiný balíček, uprav import – API (`assertThatProcessInstance`, `isCompleted`, `hasCompletedElements`, `isWaitingForMessage`, `hasVariable`, `setAssertionTimeout`) je zdokumentované pro 8.8+.
> - Testy potřebují běžící Docker (Kafka + Camunda kontejner).

Run: `cd order-process && mvn -q test -Dtest=OrderProcessIntegrationTest`
Expected: FAIL – deployment nenajde žádný BPMN / proces `order-fulfillment` neexistuje (start zprávou nic nespustí, `awaitCommand` vyprší s `AssertionError: No matching record in payments.commands`).

- [ ] **Step 2: BPMN model**

`src/main/resources/bpmn/order-fulfillment.bpmn` (otevíratelný v Camunda Desktop Modeleru – obsahuje i diagram):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
                  xmlns:modeler="http://camunda.org/schema/modeler/1.0"
                  id="Definitions_order_fulfillment"
                  targetNamespace="http://bpmn.io/schema/bpmn"
                  modeler:executionPlatform="Camunda Cloud"
                  modeler:executionPlatformVersion="8.10.0">
  <bpmn:process id="order-fulfillment" name="Order fulfillment" isExecutable="true">
    <bpmn:startEvent id="order_created" name="Order created">
      <bpmn:outgoing>flow_to_request_payment</bpmn:outgoing>
      <bpmn:messageEventDefinition id="order_created_definition" messageRef="message_order_created" />
    </bpmn:startEvent>
    <bpmn:serviceTask id="request_payment" name="Request payment">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="request-payment" retries="3" />
      </bpmn:extensionElements>
      <bpmn:incoming>flow_to_request_payment</bpmn:incoming>
      <bpmn:outgoing>flow_to_payment_result</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:intermediateCatchEvent id="payment_result" name="Payment result">
      <bpmn:incoming>flow_to_payment_result</bpmn:incoming>
      <bpmn:outgoing>flow_to_gateway</bpmn:outgoing>
      <bpmn:messageEventDefinition id="payment_result_definition" messageRef="message_payment_result" />
    </bpmn:intermediateCatchEvent>
    <bpmn:exclusiveGateway id="payment_completed_gateway" name="Payment completed?" default="flow_failed">
      <bpmn:incoming>flow_to_gateway</bpmn:incoming>
      <bpmn:outgoing>flow_completed</bpmn:outgoing>
      <bpmn:outgoing>flow_failed</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:serviceTask id="confirm_order" name="Confirm order">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="confirm-order" retries="3" />
      </bpmn:extensionElements>
      <bpmn:incoming>flow_completed</bpmn:incoming>
      <bpmn:outgoing>flow_to_confirmed</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="cancel_order" name="Cancel order">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="cancel-order" retries="3" />
      </bpmn:extensionElements>
      <bpmn:incoming>flow_failed</bpmn:incoming>
      <bpmn:outgoing>flow_to_cancelled</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:endEvent id="order_confirmed" name="Order confirmed">
      <bpmn:incoming>flow_to_confirmed</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:endEvent id="order_cancelled" name="Order cancelled">
      <bpmn:incoming>flow_to_cancelled</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="flow_to_request_payment" sourceRef="order_created" targetRef="request_payment" />
    <bpmn:sequenceFlow id="flow_to_payment_result" sourceRef="request_payment" targetRef="payment_result" />
    <bpmn:sequenceFlow id="flow_to_gateway" sourceRef="payment_result" targetRef="payment_completed_gateway" />
    <bpmn:sequenceFlow id="flow_completed" name="yes" sourceRef="payment_completed_gateway" targetRef="confirm_order">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=paymentStatus = "COMPLETED"</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="flow_failed" name="no" sourceRef="payment_completed_gateway" targetRef="cancel_order" />
    <bpmn:sequenceFlow id="flow_to_confirmed" sourceRef="confirm_order" targetRef="order_confirmed" />
    <bpmn:sequenceFlow id="flow_to_cancelled" sourceRef="cancel_order" targetRef="order_cancelled" />
  </bpmn:process>
  <bpmn:message id="message_order_created" name="OrderCreated" />
  <bpmn:message id="message_payment_result" name="PaymentResult">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=orderId" />
    </bpmn:extensionElements>
  </bpmn:message>
  <bpmndi:BPMNDiagram id="diagram_order_fulfillment">
    <bpmndi:BPMNPlane id="plane_order_fulfillment" bpmnElement="order-fulfillment">
      <bpmndi:BPMNShape id="order_created_di" bpmnElement="order_created">
        <dc:Bounds x="152" y="102" width="36" height="36" />
        <bpmndi:BPMNLabel><dc:Bounds x="136" y="145" width="68" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="request_payment_di" bpmnElement="request_payment">
        <dc:Bounds x="240" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="payment_result_di" bpmnElement="payment_result">
        <dc:Bounds x="392" y="102" width="36" height="36" />
        <bpmndi:BPMNLabel><dc:Bounds x="373" y="145" width="74" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="payment_completed_gateway_di" bpmnElement="payment_completed_gateway" isMarkerVisible="true">
        <dc:Bounds x="485" y="95" width="50" height="50" />
        <bpmndi:BPMNLabel><dc:Bounds x="460" y="65" width="100" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="confirm_order_di" bpmnElement="confirm_order">
        <dc:Bounds x="600" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="cancel_order_di" bpmnElement="cancel_order">
        <dc:Bounds x="600" y="200" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="order_confirmed_di" bpmnElement="order_confirmed">
        <dc:Bounds x="762" y="102" width="36" height="36" />
        <bpmndi:BPMNLabel><dc:Bounds x="740" y="145" width="80" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="order_cancelled_di" bpmnElement="order_cancelled">
        <dc:Bounds x="762" y="222" width="36" height="36" />
        <bpmndi:BPMNLabel><dc:Bounds x="740" y="265" width="80" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="flow_to_request_payment_di" bpmnElement="flow_to_request_payment">
        <di:waypoint x="188" y="120" /><di:waypoint x="240" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_to_payment_result_di" bpmnElement="flow_to_payment_result">
        <di:waypoint x="340" y="120" /><di:waypoint x="392" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_to_gateway_di" bpmnElement="flow_to_gateway">
        <di:waypoint x="428" y="120" /><di:waypoint x="485" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_completed_di" bpmnElement="flow_completed">
        <di:waypoint x="535" y="120" /><di:waypoint x="600" y="120" />
        <bpmndi:BPMNLabel><dc:Bounds x="559" y="102" width="18" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_failed_di" bpmnElement="flow_failed">
        <di:waypoint x="510" y="145" /><di:waypoint x="510" y="240" /><di:waypoint x="600" y="240" />
        <bpmndi:BPMNLabel><dc:Bounds x="518" y="190" width="14" height="14" /></bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_to_confirmed_di" bpmnElement="flow_to_confirmed">
        <di:waypoint x="700" y="120" /><di:waypoint x="762" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="flow_to_cancelled_di" bpmnElement="flow_to_cancelled">
        <di:waypoint x="700" y="240" /><di:waypoint x="762" y="240" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
```

`KafkaTestcontainer.java`, `KafkaTestSupport.java`: zkopírovat z `order-service/src/test/java/cz/demo/eda/order/`, změnit balíček na `cz.demo.eda.process`.

- [ ] **Step 3: Testy**

Run: `cd order-process && mvn -q test`
Expected: PASS – všech 7 testů `OrderProcessIntegrationTest` + unit testy.

- [ ] **Step 4: Celý build**

Run (z kořene): `mvn -q test`
Expected: PASS ve všech třech modulech.

---

