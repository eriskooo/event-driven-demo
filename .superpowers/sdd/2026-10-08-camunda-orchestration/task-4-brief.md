### Task 4: `ProcessGateway` – publikace zpráv do Zeebe s deduplikací

**Files:**
- Create: `order-process/src/main/java/cz/demo/eda/process/process/ProcessMessages.java`
- Create: `order-process/src/main/java/cz/demo/eda/process/process/ProcessGateway.java`
- Test: `order-process/src/test/java/cz/demo/eda/process/process/ProcessGatewayTest.java`

**Interfaces:**
- Consumes: `ProcessProperties.messageTtl()` (Task 3)
- Produces:
  - `ProcessMessages.ORDER_CREATED = "OrderCreated"`, `PAYMENT_RESULT = "PaymentResult"`, `JOB_REQUEST_PAYMENT = "request-payment"`, `JOB_CONFIRM_ORDER = "confirm-order"`, `JOB_CANCEL_ORDER = "cancel-order"`, `PROCESS_ID = "order-fulfillment"`
  - `ProcessGateway.publish(String messageName, String correlationKey, UUID messageId, Map<String, Object> variables): boolean` – `true` publikováno, `false` duplicita (`ALREADY_EXISTS`); jiné chyby propaguje.

- [ ] **Step 1: Failing test**

```java
package cz.demo.eda.process.process;

import cz.demo.eda.process.config.ProcessProperties;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.PublishMessageCommandStep1.PublishMessageCommandStep3;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// LENIENT: společný stub řetězu v setUp test s null messageId nevyužije.
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProcessGatewayTest {

    private static final UUID MESSAGE_ID = UUID.fromString("5b7c0000-0000-4000-8000-000000000001");
    private static final Map<String, Object> VARIABLES = Map.of("orderId", "o-1");

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CamundaClient client;

    private PublishMessageCommandStep3 command;
    private ProcessGateway gateway;

    @BeforeEach
    void setUp() {
        gateway = new ProcessGateway(client, new ProcessProperties(Duration.ofMinutes(1)));
        command = client.newPublishMessageCommand().messageName("PaymentResult").correlationKey("o-1");
        when(command.messageId(MESSAGE_ID.toString()).timeToLive(Duration.ofMinutes(1)).variables(VARIABLES))
                .thenReturn(command);
    }

    @Test
    @DisplayName("Publikuje zprávu s názvem, correlation key, messageId, TTL a proměnnými")
    void should_publishMessage_whenCalled() {
        boolean published = gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES);

        assertThat(published).isTrue();
        verify(command).messageId(MESSAGE_ID.toString());
        verify(command).send();
    }

    @Test
    @DisplayName("Vrátí false když Zeebe zprávu se stejným messageId už má (duplicitní doručení z Kafky)")
    void should_returnFalse_whenMessageAlreadyExists() {
        when(command.send().join()).thenThrow(new ClientStatusException(Status.ALREADY_EXISTS, null));

        assertThat(gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES)).isFalse();
    }

    @Test
    @DisplayName("Jinou chybu klienta propaguje, aby Kafka error handler zprávu zopakoval")
    void should_rethrow_whenOtherClientError() {
        when(command.send().join()).thenThrow(new ClientStatusException(Status.UNAVAILABLE, null));

        assertThatThrownBy(() -> gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES))
                .isInstanceOf(ClientStatusException.class);
    }

    @Test
    @DisplayName("Odmítne null messageId – bez něj by Zeebe neuměl deduplikovat")
    void should_throw_whenMessageIdIsNull() {
        assertThatThrownBy(() -> gateway.publish("PaymentResult", "o-1", null, VARIABLES))
                .isInstanceOf(NullPointerException.class);
    }
}
```

> Poznámka pro implementátora: řetěz kroků je `PublishMessageCommandStep1` → `messageName(..)` vrací `PublishMessageCommandStep2` → `correlationKey(..)` vrací `PublishMessageCommandStep3`. Pokud se vnořený typ v 8.10.2 jmenuje jinak, uprav import podle IDE (`client.newPublishMessageCommand().messageName("x").correlationKey("y")` – návratový typ). Konstruktor `ClientStatusException(Status, Throwable)` je v `io.camunda.client.api.command`.

Run: `cd order-process && mvn -q test -Dtest=ProcessGatewayTest`
Expected: FAIL – `cannot find symbol: class ProcessGateway`.

- [ ] **Step 2: Implementace**

`process/ProcessMessages.java`:

```java
package cz.demo.eda.process.process;

/** Názvy z BPMN modelu order-fulfillment – musí odpovídat souboru bpmn/order-fulfillment.bpmn. */
public final class ProcessMessages {

    public static final String PROCESS_ID = "order-fulfillment";
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String PAYMENT_RESULT = "PaymentResult";
    public static final String JOB_REQUEST_PAYMENT = "request-payment";
    public static final String JOB_CONFIRM_ORDER = "confirm-order";
    public static final String JOB_CANCEL_ORDER = "cancel-order";

    private ProcessMessages() {
    }
}
```

`process/ProcessGateway.java`:

```java
package cz.demo.eda.process.process;

import cz.demo.eda.process.config.ProcessProperties;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.ClientStatusException;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Jediné místo, kde služba posílá zprávy do Zeebe; idempotenci zajišťuje messageId = eventId z Kafky. */
@Component
public class ProcessGateway {

    private static final Logger log = LoggerFactory.getLogger(ProcessGateway.class);

    private final CamundaClient client;
    private final ProcessProperties properties;

    public ProcessGateway(CamundaClient client, ProcessProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    /**
     * Publikuje zprávu do Zeebe a počká na potvrzení. Zeebe ji zkoreluje s instancí čekající na stejný
     * název a correlation key, nebo ji po dobu TTL podrží.
     *
     * @return false, pokud zpráva se stejným messageId už v Zeebe je (duplicitní doručení)
     */
    public boolean publish(String messageName, String correlationKey, UUID messageId, Map<String, Object> variables) {
        Objects.requireNonNull(messageId, "messageId");
        try {
            client.newPublishMessageCommand()
                    .messageName(messageName)
                    .correlationKey(correlationKey)
                    .messageId(messageId.toString())
                    .timeToLive(properties.messageTtl())
                    .variables(variables)
                    .send()
                    .join();
            log.info("Message {} published for {} (messageId {})", messageName, correlationKey, messageId);
            return true;
        } catch (ClientStatusException e) {
            if (e.getStatusCode() != Status.Code.ALREADY_EXISTS) {
                throw e;
            }
            log.info("Duplicate message {} for {} (messageId {}) skipped", messageName, correlationKey, messageId);
            return false;
        }
    }
}
```

- [ ] **Step 3: Testy**

Run: `cd order-process && mvn -q test -Dtest=ProcessGatewayTest`
Expected: PASS.

---

