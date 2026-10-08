### Task 6: Odesílání příkazů a job workery

**Files:**
- Create: `order-process/src/main/java/cz/demo/eda/process/messaging/CommandPublisher.java`, `CommandPublishException.java`
- Create: `order-process/src/main/java/cz/demo/eda/process/process/OrderVariables.java`
- Create: `order-process/src/main/java/cz/demo/eda/process/worker/RequestPaymentWorker.java`, `ConfirmOrderWorker.java`, `CancelOrderWorker.java`
- Test: `messaging/CommandPublisherTest.java`, `worker/RequestPaymentWorkerTest.java`, `worker/ConfirmOrderWorkerTest.java`, `worker/CancelOrderWorkerTest.java`

**Interfaces:**
- Consumes: proměnné procesu z Task 5, `ProcessMessages.JOB_*` (Task 4), `ProcessPayment`, `ConfirmOrder`, `CancelOrder` (Task 3), `Topics`, `Tracing`
- Produces:
  - `CommandPublisher.send(String topic, String key, Object command, String correlationId): void` – synchronní, při chybě `CommandPublishException`
  - `CommandPublisher.commandId(long jobKey): UUID` – deterministické
  - `OrderVariables(String orderId, BigDecimal amount, String currency, String correlationId, String paymentStatus, String paymentId, String failureReason)`

- [ ] **Step 1: Failing testy**

`messaging/CommandPublisherTest.java`:

```java
package cz.demo.eda.process.messaging;

import cz.demo.eda.process.support.Tracing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandPublisherTest {

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Test
    @DisplayName("Odešle příkaz s klíčem a correlationId v hlavičce a počká na potvrzení brokeru")
    @SuppressWarnings("unchecked")
    void should_sendRecordWithHeader_whenCorrelationIdPresent() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        new CommandPublisher(kafkaTemplate).send("payments.commands", "o-1", "payload", "corr-1");

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<Object, Object> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("payments.commands");
        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.value()).isEqualTo("payload");
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-1");
    }

    @Test
    @DisplayName("Bez correlationId hlavičku nepřidá")
    @SuppressWarnings("unchecked")
    void should_omitHeader_whenCorrelationIdNull() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        new CommandPublisher(kafkaTemplate).send("orders.commands", "o-1", "payload", null);

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().headers().lastHeader(Tracing.CORRELATION_ID_HEADER)).isNull();
    }

    @Test
    @DisplayName("Selhání odeslání převede na CommandPublishException – job se nedokončí a Zeebe ho zopakuje")
    @SuppressWarnings("unchecked")
    void should_throw_whenSendFails() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> new CommandPublisher(kafkaTemplate).send("orders.commands", "o-1", "p", "c"))
                .isInstanceOf(CommandPublishException.class)
                .hasMessageContaining("orders.commands");
    }

    @Test
    @DisplayName("Stejný jobKey dá vždy stejné commandId, různé jobKey různá")
    void should_deriveStableCommandId_whenSameJobKey() {
        assertThat(CommandPublisher.commandId(42L)).isEqualTo(CommandPublisher.commandId(42L));
        assertThat(CommandPublisher.commandId(42L)).isNotEqualTo(CommandPublisher.commandId(43L));
    }

    @Test
    @DisplayName("Pro hraniční jobKey 0 i Long.MAX_VALUE vrátí platné UUID")
    void should_returnUuid_whenJobKeyAtBoundaries() {
        assertThat(CommandPublisher.commandId(0L)).isNotNull();
        assertThat(CommandPublisher.commandId(Long.MAX_VALUE)).isNotEqualTo(CommandPublisher.commandId(0L));
    }
}
```

`worker/RequestPaymentWorkerTest.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ProcessPayment;
import cz.demo.eda.process.messaging.CommandPublishException;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.api.response.ActivatedJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestPaymentWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    private RequestPaymentWorker worker;

    @BeforeEach
    void setUp() {
        worker = new RequestPaymentWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC));
        when(job.getKey()).thenReturn(42L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", new BigDecimal("10.50"), "CZK", "corr-1", null, null, null));
    }

    @Test
    @DisplayName("Pošle ProcessPayment do payments.commands s eventId odvozeným z jobKey")
    void should_sendProcessPayment_whenJobActivated() {
        worker.requestPayment(job);

        ArgumentCaptor<Object> command = ArgumentCaptor.forClass(Object.class);
        verify(publisher).send(eq(Topics.PAYMENTS_COMMANDS), eq("o-1"), command.capture(), eq("corr-1"));
        assertThat(command.getValue()).isEqualTo(new ProcessPayment(CommandPublisher.commandId(42L), NOW, "corr-1",
                "o-1", new BigDecimal("10.50"), "CZK"));
    }

    @Test
    @DisplayName("Selhání odeslání propaguje – job zůstane nedokončený a Zeebe sníží retries")
    void should_propagate_whenPublishFails() {
        doThrow(new CommandPublishException("payments.commands", new IllegalStateException("down")))
                .when(publisher).send(anyString(), anyString(), any(), anyString());

        assertThatThrownBy(() -> worker.requestPayment(job)).isInstanceOf(CommandPublishException.class);
    }
}
```

`worker/ConfirmOrderWorkerTest.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ConfirmOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.api.response.ActivatedJob;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfirmOrderWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    @Test
    @DisplayName("Pošle ConfirmOrder do orders.commands s paymentId z procesu")
    void should_sendConfirmOrder_whenJobActivated() {
        when(job.getKey()).thenReturn(7L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", BigDecimal.TEN, "CZK", "corr-1", "COMPLETED", "p-1", null));

        new ConfirmOrderWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC)).confirmOrder(job);

        verify(publisher).send(Topics.ORDERS_COMMANDS, "o-1",
                new ConfirmOrder(CommandPublisher.commandId(7L), NOW, "corr-1", "o-1", "p-1"), "corr-1");
    }
}
```

`worker/CancelOrderWorkerTest.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.CancelOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.api.response.ActivatedJob;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CancelOrderWorkerTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:00:00Z");

    @Mock
    private CommandPublisher publisher;
    @Mock
    private ActivatedJob job;

    @Test
    @DisplayName("Pošle CancelOrder do orders.commands s důvodem zamítnutí")
    void should_sendCancelOrder_whenJobActivated() {
        when(job.getKey()).thenReturn(8L);
        when(job.getVariablesAsType(OrderVariables.class)).thenReturn(
                new OrderVariables("o-1", BigDecimal.TEN, "CZK", null, "FAILED", null, "declined"));

        new CancelOrderWorker(publisher, Clock.fixed(NOW, ZoneOffset.UTC)).cancelOrder(job);

        verify(publisher).send(Topics.ORDERS_COMMANDS, "o-1",
                new CancelOrder(CommandPublisher.commandId(8L), NOW, null, "o-1", "declined"), null);
    }
}
```

Run: `cd order-process && mvn -q test`
Expected: FAIL – chybí `CommandPublisher`, `OrderVariables`, workery.

- [ ] **Step 2: Implementace**

`messaging/CommandPublishException.java`:

```java
package cz.demo.eda.process.messaging;

/** Příkaz se nepodařilo předat Kafce; job zůstane nedokončený a Zeebe ho zopakuje. */
public class CommandPublishException extends RuntimeException {

    /** Vytvoří výjimku pro topic, do kterého se nepodařilo zapsat. */
    public CommandPublishException(String topic, Throwable cause) {
        super("Failed to publish command to " + topic, cause);
    }
}
```

`messaging/CommandPublisher.java`:

```java
package cz.demo.eda.process.messaging;

import cz.demo.eda.process.support.Tracing;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Odesílá příkazy do Kafky synchronně – job worker smí job dokončit až po potvrzení brokeru,
 * jinak by se při pádu příkaz ztratil. Outbox tu není potřeba: opakování zajistí Zeebe (job se rozdá znovu).
 */
@Component
public class CommandPublisher {

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public CommandPublisher(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Deterministické ID příkazu z klíče jobu: opakovaný job pošle příkaz se stejným eventId
     * a inbox příjemce ho zahodí jako duplicitu.
     */
    public static UUID commandId(long jobKey) {
        return UUID.nameUUIDFromBytes(("job-" + jobKey).getBytes(StandardCharsets.UTF_8));
    }

    /** Odešle příkaz a počká na potvrzení brokeru; při chybě vyhodí {@link CommandPublishException}. */
    public void send(String topic, String key, Object command, String correlationId) {
        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, key, command);
        if (correlationId != null) {
            record.headers().add(Tracing.CORRELATION_ID_HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            kafkaTemplate.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandPublishException(topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CommandPublishException(topic, e);
        }
    }
}
```

`process/OrderVariables.java`:

```java
package cz.demo.eda.process.process;

import java.math.BigDecimal;

/**
 * Proměnné instance procesu order-fulfillment, jak je čtou job workery.
 * Pole odpovídají proměnným, které zakládají listenery (OrderCreated, PaymentResult).
 */
public record OrderVariables(
        String orderId,
        BigDecimal amount,
        String currency,
        String correlationId,
        String paymentStatus,
        String paymentId,
        String failureReason) {
}
```

`worker/RequestPaymentWorker.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ProcessPayment;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

/** Service task „Request payment“: pošle payment-service příkaz k platbě. */
@Component
public class RequestPaymentWorker {

    private static final Logger log = LoggerFactory.getLogger(RequestPaymentWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    public RequestPaymentWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle ProcessPayment; job se dokončí automaticky po návratu (tedy po ack Kafky). */
    @JobWorker(type = ProcessMessages.JOB_REQUEST_PAYMENT)
    public void requestPayment(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        ProcessPayment command = new ProcessPayment(CommandPublisher.commandId(job.getKey()), clock.instant(),
                variables.correlationId(), variables.orderId(), variables.amount(), variables.currency());
        publisher.send(Topics.PAYMENTS_COMMANDS, variables.orderId(), command, variables.correlationId());
        log.info("ProcessPayment {} sent for order {}", command.eventId(), variables.orderId());
    }
}
```

`worker/ConfirmOrderWorker.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.ConfirmOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

/** Service task „Confirm order“: pošle order-service příkaz k potvrzení objednávky. */
@Component
public class ConfirmOrderWorker {

    private static final Logger log = LoggerFactory.getLogger(ConfirmOrderWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    public ConfirmOrderWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle ConfirmOrder s paymentId z výsledku platby. */
    @JobWorker(type = ProcessMessages.JOB_CONFIRM_ORDER)
    public void confirmOrder(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        ConfirmOrder command = new ConfirmOrder(CommandPublisher.commandId(job.getKey()), clock.instant(),
                variables.correlationId(), variables.orderId(), variables.paymentId());
        publisher.send(Topics.ORDERS_COMMANDS, variables.orderId(), command, variables.correlationId());
        log.info("ConfirmOrder {} sent for order {}", command.eventId(), variables.orderId());
    }
}
```

`worker/CancelOrderWorker.java`:

```java
package cz.demo.eda.process.worker;

import cz.demo.eda.process.event.CancelOrder;
import cz.demo.eda.process.messaging.CommandPublisher;
import cz.demo.eda.process.process.OrderVariables;
import cz.demo.eda.process.process.ProcessMessages;
import cz.demo.eda.process.support.Topics;
import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;

/** Service task „Cancel order“: pošle order-service příkaz ke zrušení objednávky. */
@Component
public class CancelOrderWorker {

    private static final Logger log = LoggerFactory.getLogger(CancelOrderWorker.class);

    private final CommandPublisher publisher;
    private final Clock clock;

    public CancelOrderWorker(CommandPublisher publisher, Clock clock) {
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Odešle CancelOrder s důvodem zamítnutí platby. */
    @JobWorker(type = ProcessMessages.JOB_CANCEL_ORDER)
    public void cancelOrder(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        CancelOrder command = new CancelOrder(CommandPublisher.commandId(job.getKey()), clock.instant(),
                variables.correlationId(), variables.orderId(), variables.failureReason());
        publisher.send(Topics.ORDERS_COMMANDS, variables.orderId(), command, variables.correlationId());
        log.info("CancelOrder {} sent for order {}", command.eventId(), variables.orderId());
    }
}
```

- [ ] **Step 3: Testy**

Run: `cd order-process && mvn -q test`
Expected: PASS.

---

