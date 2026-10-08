### Task 1: payment-service přijímá příkaz `ProcessPayment` z `payments.commands`

**Files:**
- Create: `payment-service/src/main/java/cz/demo/eda/payment/event/ProcessPayment.java`
- Delete: `payment-service/src/main/java/cz/demo/eda/payment/event/OrderCreated.java`
- Create: `payment-service/src/main/java/cz/demo/eda/payment/messaging/ProcessPaymentListener.java`, `ProcessPaymentHandler.java`
- Delete: `payment-service/src/main/java/cz/demo/eda/payment/messaging/OrderCreatedListener.java`, `OrderCreatedHandler.java`
- Modify: `support/Topics.java`, `config/KafkaConfig.java`, `messaging/DeadLetterListener.java`, `domain/PaymentSimulator.java`, `domain/Payment.java`, `domain/PaymentService.java`, `src/main/resources/application.yml`, `pom.xml` (description)
- Test: rename `messaging/OrderCreatedListenerTest.java` → `ProcessPaymentListenerTest.java`, `OrderCreatedHandlerTest.java` → `ProcessPaymentHandlerTest.java`; modify `event/EventSerializationTest.java`, `domain/PaymentServiceTest.java`, `domain/PaymentSimulatorTest.java`, `domain/PaymentTest.java`, `outbox/OutboxPublisherTest.java`, `messaging/DeadLetterListenerTest.java`, `support/TopicsTest.java`, `PaymentServiceIntegrationTest.java`

**Interfaces:**
- Produces (JSON kontrakt `payments.commands`, konzumuje ho order-process v Task 6): `{"eventId","timestamp","correlationId","orderId","amount","currency"}`
- `payments.result` beze změny.

- [ ] **Step 1: Přepsat testy na nový kontrakt (failing)**

Mechanické náhrady ve všech testech `payment-service/src/test/java` (kromě listener/handler testů, které se přejmenují níže):
- `import cz.demo.eda.payment.event.OrderCreated;` → `import cz.demo.eda.payment.event.ProcessPayment;`
- typ `OrderCreated` → `ProcessPayment`
- `OrderCreated.of(c, orderId, customerId, amount, currency)` → `ProcessPayment.of(c, orderId, amount, currency)` (vypustit 3. argument customerId), např. `OrderCreated.of("c", "o-1", "cust", new BigDecimal(amount), "CZK")` → `ProcessPayment.of("c", "o-1", new BigDecimal(amount), "CZK")`
- `Topics.ORDERS_CREATED` → `Topics.PAYMENTS_COMMANDS`, řetězec `"orders.created"` → `"payments.commands"`, `"orders.created.DLT"` → `"payments.commands.DLT"`
- `@DisplayName`/názvy testů obsahující „OrderCreated“ přepsat na „ProcessPayment“ (např. `should_roundTripOrderCreated_whenSerialized` → `should_roundTripProcessPayment_whenSerialized`, „OrderCreated projde JSON round-tripem…“ → „ProcessPayment projde JSON round-tripem…“).

`git mv` listener/handler testů a uvnitř:
- `OrderCreatedListenerTest` → `ProcessPaymentListenerTest`, `OrderCreatedListener` → `ProcessPaymentListener`, `listener.onOrderCreated(` → `listener.onProcessPayment(`, helper `order()` vrací `ProcessPayment.of("c", "o-1", BigDecimal.TEN, "CZK")`, očekávaný topic `"payments.commands"`, DisplayName „Duplicitní OrderCreated…“ → „Duplicitní ProcessPayment…“.
- `OrderCreatedHandlerTest` → `ProcessPaymentHandlerTest`, `OrderCreatedHandler` → `ProcessPaymentHandler`, `OrderCreated` → `ProcessPayment`.

V `PaymentServiceIntegrationTest` navíc přidat test, že stará cesta je odpojená:

```java
    @Test
    @DisplayName("Zpráva v orders.created už platbu nespustí – platbu řídí jen příkaz z payments.commands")
    void should_ignoreOrdersCreated_whenOrchestrated() {
        KafkaTestSupport.sendRaw(kafka.getBootstrapServers(), "orders.created", "order-legacy",
                "{\"eventId\":\"6f1c2d3e-0000-4000-8000-000000000001\",\"timestamp\":\"2026-10-08T10:00:00Z\","
                        + "\"orderId\":\"order-legacy\",\"customerId\":\"c\",\"amount\":10,\"currency\":\"CZK\"}");

        assertThat(KafkaTestSupport.collectRecords(kafka.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-legacy".equals(r.key()), Duration.ofSeconds(3))).isEmpty();
    }
```

(`orders.created` v testcontaineru neexistuje, protože ho payment-service už nezakládá; `sendRaw` ho vytvoří automaticky – Testcontainers Kafka má auto-create zapnutý.)

- [ ] **Step 2: Ověřit, že testy nejdou přeložit**

Run: `cd payment-service && mvn -q test`
Expected: FAIL – compilation error `cannot find symbol: class ProcessPayment`.

- [ ] **Step 3: Implementace**

`event/ProcessPayment.java`:

```java
package cz.demo.eda.payment.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Příkaz od orchestrátoru order-process: proveď platbu za objednávku. */
public record ProcessPayment(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        BigDecimal amount,
        String currency) implements DomainEvent {

    public ProcessPayment {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(amount, "amount");
    }

    /** Vytvoří nový příkaz s vygenerovaným eventId a aktuálním časem. */
    public static ProcessPayment of(String correlationId, String orderId, BigDecimal amount, String currency) {
        return new ProcessPayment(UUID.randomUUID(), Instant.now(), correlationId, orderId, amount, currency);
    }
}
```

`support/Topics.java` – konstanta `ORDERS_CREATED` se nahradí:

```java
    public static final String PAYMENTS_COMMANDS = "payments.commands";
    public static final String PAYMENTS_RESULT = "payments.result";
```

`config/KafkaConfig.java`:
- JavaDoc třídy: `/** Topicy, retry s backoffem a dead letter topic pro konzumenta payments.commands. */`
- `edaTopics`: `topic(Topics.PAYMENTS_COMMANDS, settings), topic(Topics.PAYMENTS_RESULT, settings), topic(Topics.dltOf(Topics.PAYMENTS_COMMANDS), settings)`
- `kafkaErrorHandler`: `metrics.deadLetterCounter(Topics.dltOf(Topics.PAYMENTS_COMMANDS));`

`messaging/DeadLetterListener.java`:

```java
    private static final String DLT_TOPIC = Topics.PAYMENTS_COMMANDS + Topics.DLT_SUFFIX;

    /** Zaloguje zprávu z DLT včetně důvodu selhání. */
    @KafkaListener(id = "process-payment-dlt-listener", idIsGroup = false, topics = DLT_TOPIC, groupId = "payment-service-dlt",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
```

`messaging/ProcessPaymentListener.java` – kopie `OrderCreatedListener` s těmito změnami (celý soubor):

```java
package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.inbox.InboxEntry;
import cz.demo.eda.payment.inbox.InboxService;
import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

/**
 * Přijímá příkazy k platbě od orchestrátoru a jen je uloží do inboxu. Offset potvrdí ručně
 * (AckMode.MANUAL_IMMEDIATE) až po úspěšném uložení; samotnou platbu s retry provede InboxProcessor.
 */
@Component
public class ProcessPaymentListener {

    private static final Logger log = LoggerFactory.getLogger(ProcessPaymentListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public ProcessPaymentListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží příkaz do inboxu a potvrdí offset; duplicitní eventId (opakovaný job v Zeebe) jen potvrdí. */
    @KafkaListener(id = "process-payment-listener", idIsGroup = false, topics = Topics.PAYMENTS_COMMANDS)
    public void onProcessPayment(ConsumerRecord<String, ProcessPayment> record, Acknowledgment ack) {
        ProcessPayment command = record.value();
        boolean stored = inbox.store(InboxEntry.received(command.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(command), correlationId(record, command), clock.instant()));
        if (stored) {
            log.info("ProcessPayment {} for order {} stored in inbox", command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
        } else {
            log.info("Duplicate ProcessPayment {} for order {} skipped", command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
        }
        ack.acknowledge();
    }

    private static String correlationId(ConsumerRecord<?, ?> record, ProcessPayment command) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : command.correlationId();
    }
}
```

`messaging/ProcessPaymentHandler.java`:

```java
package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.domain.PaymentService;
import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.inbox.InboxEntry;
import cz.demo.eda.payment.inbox.InboxMessageHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Zpracuje příkaz k platbě uložený v inboxu. */
@Component
public class ProcessPaymentHandler implements InboxMessageHandler {

    private final PaymentService paymentService;
    private final JsonMapper jsonMapper;

    public ProcessPaymentHandler(PaymentService paymentService, JsonMapper jsonMapper) {
        this.paymentService = paymentService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void handle(InboxEntry entry) {
        paymentService.processPayment(jsonMapper.readValue(entry.payload(), ProcessPayment.class));
    }
}
```

`domain/PaymentSimulator.java`, `domain/Payment.java`, `domain/PaymentService.java`: import `OrderCreated` → `ProcessPayment`, typ parametru `OrderCreated order` → `ProcessPayment order` (těla metod se nemění – `ProcessPayment` má `orderId()`, `amount()`, `currency()`, `correlationId()`). JavaDoc `PaymentService.processPayment`: „Provede platbu za objednávku z příkazu orchestrátoru, uloží ji a zařadí výsledek do outboxu – vše v jedné transakci…“.

`application.yml`: `spring.json.value.default.type: cz.demo.eda.payment.event.ProcessPayment`.

`pom.xml` `<description>`: `Simuluje platby na příkaz orchestrátoru a publikuje jejich výsledek`.

Smazat `event/OrderCreated.java`, `messaging/OrderCreatedListener.java`, `messaging/OrderCreatedHandler.java`.

- [ ] **Step 4: Testy a kontrola zbytků**

Run: `cd payment-service && mvn -q test`
Expected: PASS (vč. integračních testů s Testcontainers – vyžaduje běžící Docker).

Run: `grep -rn "OrderCreated\|ORDERS_CREATED" payment-service/src` (z kořene repa)
Expected: žádný výskyt kromě nového testu `should_ignoreOrdersCreated_whenOrchestrated` (řetězec `orders.created`).

---

