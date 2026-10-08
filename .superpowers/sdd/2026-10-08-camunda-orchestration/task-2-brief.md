### Task 2: order-service přijímá příkazy `ConfirmOrder` / `CancelOrder` z `orders.commands`

**Files:**
- Create: `order-service/src/main/java/cz/demo/eda/order/event/OrderCommand.java`, `ConfirmOrder.java`, `CancelOrder.java`
- Delete: `order-service/src/main/java/cz/demo/eda/order/event/PaymentResult.java`, `PaymentCompleted.java`, `PaymentFailed.java`
- Create: `order-service/src/main/java/cz/demo/eda/order/messaging/OrderCommandListener.java`, `OrderCommandHandler.java`
- Delete: `order-service/src/main/java/cz/demo/eda/order/messaging/PaymentResultListener.java`, `PaymentResultHandler.java`
- Modify: `support/Topics.java`, `config/KafkaConfig.java`, `domain/OrderService.java`, `src/main/resources/application.yml`, `pom.xml` (description)
- Test: create `messaging/OrderCommandListenerTest.java`, `messaging/OrderCommandHandlerTest.java` (delete `PaymentResult*Test`); modify `event/EventSerializationTest.java`, `domain/OrderServiceTest.java`, `inbox/InboxRepositoryTest.java`, `OrderServiceIntegrationTest.java`

**Interfaces:**
- Produces (JSON kontrakt `orders.commands`, produkuje ho order-process v Task 6):
  `{"type":"ConfirmOrder","eventId","timestamp","correlationId","orderId","paymentId"}`, `{"type":"CancelOrder","eventId","timestamp","correlationId","orderId","reason"}`
- `OrderService.applyCommand(OrderCommand command): Optional<Order>` (nahrazuje `applyPaymentResult`).

- [ ] **Step 1: Napsat failing testy**

`event/EventSerializationTest.java` – test `should_deserializeSubtype_whenReadAsPaymentResult` nahradit, `should_generateUniqueEventIds_whenCreatedTwice` upravit:

```java
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
```

`domain/OrderServiceTest.java` – mechanické náhrady:
- importy `PaymentCompleted`, `PaymentFailed` → `ConfirmOrder`, `CancelOrder`
- `service.applyPaymentResult(PaymentCompleted.of("corr", "o-1", "p-1", BigDecimal.TEN))` → `service.applyCommand(ConfirmOrder.of("corr", "o-1", "p-1"))`
- `service.applyPaymentResult(PaymentFailed.of("corr", X, Y))` → `service.applyCommand(CancelOrder.of("corr", X, Y))`
- DisplayName „PaymentCompleted převede…“ → „ConfirmOrder převede…“, „PaymentFailed převede…“ → „CancelOrder převede…“; metody `should_markPaid_whenPaymentCompleted` → `should_markPaid_whenConfirmOrder`, `should_markFailed_whenPaymentFailed` → `should_markFailed_whenCancelOrder`. (Pokud po náhradě zůstane nepoužitý import `BigDecimal`, odstranit ho.)

`inbox/InboxRepositoryTest.java`: `PAYLOAD = "{\"orderId\": \"o-1\", \"type\": \"ConfirmOrder\"}"`.

`messaging/OrderCommandHandlerTest.java`:

```java
package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.CancelOrder;
import cz.demo.eda.order.inbox.InboxEntry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class OrderCommandHandlerTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Mock
    private OrderService orderService;

    @Test
    @DisplayName("Payload z inboxu deserializuje na správný podtyp příkazu a předá doméně")
    void should_applyDeserializedCommand_whenHandled() {
        CancelOrder command = CancelOrder.of("c", "o-1", "declined");
        OrderCommandHandler handler = new OrderCommandHandler(orderService, jsonMapper);

        handler.handle(InboxEntry.received(command.eventId(), "orders.commands", "o-1",
                jsonMapper.writeValueAsString(command), "c", Instant.now()));

        verify(orderService).applyCommand(command);
    }

    @Test
    @DisplayName("Nečitelný payload vyhodí výjimku – inbox ji zpracuje jako neúspěšný pokus")
    void should_throw_whenPayloadInvalid() {
        OrderCommandHandler handler = new OrderCommandHandler(orderService, jsonMapper);

        assertThatThrownBy(() -> handler.handle(InboxEntry.received(UUID.randomUUID(), "orders.commands", "o-1",
                "{\"type\":\"Unknown\"}", null, Instant.now()))).isInstanceOf(JacksonException.class);
        verifyNoInteractions(orderService);
    }
}
```

`messaging/OrderCommandListenerTest.java` – kopie `PaymentResultListenerTest` s náhradami: `PaymentResultListener` → `OrderCommandListener`, `onPaymentResult` → `onOrderCommand`, `PaymentResult` → `OrderCommand`, `PaymentCompleted.of("corr-body", "o-1", "p-1", BigDecimal.ONE)` / `PaymentCompleted.of("c", "o-1", "p-1", BigDecimal.ONE)` → `ConfirmOrder.of("corr-body", "o-1", "p-1")` / `ConfirmOrder.of("c", "o-1", "p-1")`, topic `"payments.result"` → `"orders.commands"`, očekávaný payload `"\"type\":\"ConfirmOrder\""`, DisplayName první metody „Příkaz uloží do inboxu s correlationId z hlavičky a typem v payloadu“, ostatní DisplayName beze změny. Odstranit import `BigDecimal`.

`OrderServiceIntegrationTest.java`:
- import `PaymentCompleted` → `ConfirmOrder`
- `PaymentCompleted payment = PaymentCompleted.of("it-corr-1", orderId, "pay-1", new BigDecimal("42.00"));` → `ConfirmOrder confirm = ConfirmOrder.of("it-corr-1", orderId, "pay-1");`, proměnné `payment` → `confirm`
- `sendPaymentResult(...)` → `sendCommand(ConfirmOrder confirm)` s topicem `Topics.ORDERS_COMMANDS`
- DisplayName testu 1: „Objednávka jde přes outbox do orders.created a ConfirmOrder ji přes inbox převede na PAID právě jednou“, metoda `should_completeOrder_whenConfirmOrderReceived`
- test 2: `Topics.PAYMENTS_RESULT` → `Topics.ORDERS_COMMANDS`, DisplayName „Nečitelná zpráva v orders.commands skončí v DLT bez retry a do inboxu se nedostane“, metoda `should_moveToDlt_whenOrderCommandUnreadable`
- odstranit nepoužitý import `BigDecimal`.

- [ ] **Step 2: Ověřit, že testy nejdou přeložit**

Run: `cd order-service && mvn -q test`
Expected: FAIL – `cannot find symbol: class OrderCommand`.

- [ ] **Step 3: Implementace**

`event/OrderCommand.java`:

```java
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
```

`event/ConfirmOrder.java`:

```java
package cz.demo.eda.order.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba proběhla – objednávku potvrdit. */
public record ConfirmOrder(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String paymentId) implements OrderCommand {

    public ConfirmOrder {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Vytvoří nový příkaz s vygenerovaným eventId a aktuálním časem. */
    public static ConfirmOrder of(String correlationId, String orderId, String paymentId) {
        return new ConfirmOrder(UUID.randomUUID(), Instant.now(), correlationId, orderId, paymentId);
    }
}
```

`event/CancelOrder.java`:

```java
package cz.demo.eda.order.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba byla zamítnuta – objednávku zrušit. */
public record CancelOrder(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String reason) implements OrderCommand {

    public CancelOrder {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(orderId, "orderId");
    }

    /** Vytvoří nový příkaz s vygenerovaným eventId a aktuálním časem. */
    public static CancelOrder of(String correlationId, String orderId, String reason) {
        return new CancelOrder(UUID.randomUUID(), Instant.now(), correlationId, orderId, reason);
    }
}
```

`support/Topics.java`: `PAYMENTS_RESULT` nahradit `public static final String ORDERS_COMMANDS = "orders.commands";`

`config/KafkaConfig.java`:
- JavaDoc třídy zmiňuje konzumenta `orders.commands`
- `edaTopics`: `topic(Topics.ORDERS_CREATED, settings), topic(Topics.ORDERS_COMMANDS, settings), topic(Topics.dltOf(Topics.ORDERS_COMMANDS), settings)`
- `metrics.deadLetterCounter(Topics.dltOf(Topics.ORDERS_COMMANDS));`

`domain/OrderService.java` – JavaDoc třídy „Doménová logika objednávek: založení a provedení příkazů orchestrátoru.“; `applyPaymentResult` a `transition` nahradit:

```java
    /**
     * Provede příkaz orchestrátoru nad objednávkou; vrací prázdno pro neznámou objednávku.
     * Volá ho InboxService ve své transakci (deduplikaci už zajistil inbox).
     */
    public Optional<Order> applyCommand(OrderCommand command) {
        Optional<Order> order = repository.findForUpdate(command.orderId());
        if (order.isEmpty()) {
            // Příkaz pro neznámou objednávku retry nespraví – jen varování, zpráva se označí jako zpracovaná.
            log.warn("Command {} for unknown order {} ignored", command.eventId(), command.orderId());
            return order;
        }
        // Změna se uloží dirty checkingem při commitu transakce.
        transition(order.get(), command);
        return order;
    }

    private void transition(Order order, OrderCommand command) {
        if (order.status().isFinal()) {
            log.warn("Order {} already in final state {}, command {} ignored", order.id(), order.status(),
                    command.eventId());
            return;
        }
        OrderStatus previous = order.status();
        Instant now = clock.instant();
        switch (command) {
            case ConfirmOrder confirm -> order.markPaid(confirm.paymentId(), now);
            case CancelOrder cancel -> order.markPaymentFailed(cancel.reason(), now);
        }
        log.info("Order {} status changed {} -> {}", order.id(), previous, order.status());
    }
```

(importy `PaymentCompleted/PaymentFailed/PaymentResult` → `CancelOrder/ConfirmOrder/OrderCommand`)

`messaging/OrderCommandHandler.java`:

```java
package cz.demo.eda.order.messaging;

import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.event.OrderCommand;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.inbox.InboxMessageHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Zpracuje příkaz orchestrátoru uložený v inboxu. */
@Component
public class OrderCommandHandler implements InboxMessageHandler {

    private final OrderService orderService;
    private final JsonMapper jsonMapper;

    public OrderCommandHandler(OrderService orderService, JsonMapper jsonMapper) {
        this.orderService = orderService;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void handle(InboxEntry entry) {
        orderService.applyCommand(jsonMapper.readValue(entry.payload(), OrderCommand.class));
    }
}
```

`messaging/OrderCommandListener.java`:

```java
package cz.demo.eda.order.messaging;

import cz.demo.eda.order.event.OrderCommand;
import cz.demo.eda.order.inbox.InboxEntry;
import cz.demo.eda.order.inbox.InboxService;
import cz.demo.eda.order.support.MessagingMetrics;
import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;

/**
 * Přijímá příkazy orchestrátoru a jen je uloží do inboxu; offset se commituje (AckMode.RECORD) až po
 * úspěšném uložení. Samotné zpracování s retry obstará InboxProcessor.
 */
@Component
public class OrderCommandListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandListener.class);

    private final InboxService inbox;
    private final JsonMapper jsonMapper;
    private final MessagingMetrics metrics;
    private final Clock clock;

    public OrderCommandListener(InboxService inbox, JsonMapper jsonMapper, MessagingMetrics metrics, Clock clock) {
        this.inbox = inbox;
        this.jsonMapper = jsonMapper;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** Uloží příkaz do inboxu; duplicitní eventId (opakovaný job v Zeebe) přeskočí. */
    @KafkaListener(id = "order-command-listener", idIsGroup = false, topics = Topics.ORDERS_COMMANDS)
    public void onOrderCommand(ConsumerRecord<String, OrderCommand> record) {
        OrderCommand command = record.value();
        String type = command.getClass().getSimpleName();
        boolean stored = inbox.store(InboxEntry.received(command.eventId(), record.topic(), record.key(),
                jsonMapper.writeValueAsString(command), correlationId(record, command), clock.instant()));
        if (!stored) {
            log.info("Duplicate {} {} for order {} skipped", type, command.eventId(), command.orderId());
            metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_DUPLICATE);
            return;
        }
        log.info("Received {} {} for order {} stored in inbox", type, command.eventId(), command.orderId());
        metrics.consumed(record.topic(), MessagingMetrics.OUTCOME_STORED);
    }

    private static String correlationId(ConsumerRecord<?, ?> record, OrderCommand command) {
        Header header = record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : command.correlationId();
    }
}
```

`application.yml`: `spring.json.value.default.type: cz.demo.eda.order.event.OrderCommand`.

`pom.xml` `<description>`: `REST API objednávek, publikuje OrderCreated a provádí příkazy orchestrátoru`.

Smazat `event/PaymentResult.java`, `PaymentCompleted.java`, `PaymentFailed.java`, `messaging/PaymentResultListener.java`, `PaymentResultHandler.java` a jejich testy.

- [ ] **Step 4: Testy a kontrola zbytků**

Run: `cd order-service && mvn -q test`
Expected: PASS.

Run: `grep -rn "PaymentResult\|PaymentCompleted\|PaymentFailed\|PAYMENTS_RESULT" order-service/src`
Expected: žádný výskyt.

---

