### Task 3: Kostra služby order-process (build, konfigurace, zprávy)

**Files:**
- Create: `order-process/pom.xml`, `order-process/Dockerfile` (kopie `order-service/Dockerfile`), `order-process/.dockerignore` (kopie)
- Modify: `pom.xml` (kořenový agregátor)
- Create: `order-process/src/main/java/cz/demo/eda/process/OrderProcessApplication.java`
- Create: `config/AppConfig.java`, `config/EdaKafkaProperties.java`, `config/KafkaConfig.java`, `config/ProcessProperties.java`
- Create: `support/Topics.java`, `support/Tracing.java`, `support/CorrelationIdRecordInterceptor.java`
- Create: `event/DomainEvent.java`, `OrderCreated.java`, `PaymentResult.java`, `PaymentCompleted.java`, `PaymentFailed.java`, `ProcessPayment.java`, `OrderCommand.java`, `ConfirmOrder.java`, `CancelOrder.java`
- Create: `src/main/resources/application.yml`
- Test: `event/EventSerializationTest.java`, `support/TopicsTest.java`, `support/CorrelationIdRecordInterceptorTest.java`, `config/ProcessPropertiesTest.java`

**Interfaces:**
- Produces: `Topics.ORDERS_CREATED`, `PAYMENTS_COMMANDS`, `PAYMENTS_RESULT`, `ORDERS_COMMANDS`, `Topics.dltOf(String)`; `Tracing.CORRELATION_ID_HEADER`, `CORRELATION_ID_MDC_KEY`; `ProcessProperties(Duration messageTtl)` (`eda.process.message-ttl`, default `1m`); events níže; bean `Clock`.

- [ ] **Step 1: Build soubory**

Kořenový `pom.xml` – do `<modules>` přidat `<module>order-process</module>`.

`order-process/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.1</version>
        <relativePath/>
    </parent>

    <groupId>cz.demo.eda</groupId>
    <artifactId>order-process</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>order-process</name>
    <description>Orchestrátor objednávky: BPMN proces v Camundě řízený přes Kafku</description>

    <properties>
        <java.version>21</java.version>
        <camunda.version>8.10.2</camunda.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>io.camunda</groupId>
            <artifactId>camunda-spring-boot-starter</artifactId>
            <version>${camunda.version}</version>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-webmvc</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.camunda</groupId>
            <artifactId>camunda-process-test-spring</artifactId>
            <version>${camunda.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <finalName>app</finalName>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

Run: `cd order-process && mvn -q dependency:resolve`
Expected: BUILD SUCCESS (ověří dostupnost Camunda 8.10.2 artefaktů).

- [ ] **Step 2: Napsat failing testy**

`src/test/java/cz/demo/eda/process/event/EventSerializationTest.java`:

```java
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
```

`support/TopicsTest.java`:

```java
package cz.demo.eda.process.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsTest {

    @Test
    @DisplayName("Vrátí název DLT topicu s příponou .DLT")
    void should_appendDltSuffix_whenTopicGiven() {
        assertThat(Topics.dltOf(Topics.PAYMENTS_RESULT)).isEqualTo("payments.result.DLT");
    }

    @Test
    @DisplayName("Pro prázdný název vrátí jen příponu")
    void should_returnSuffixOnly_whenTopicIsEmpty() {
        assertThat(Topics.dltOf("")).isEqualTo(".DLT");
    }
}
```

`support/CorrelationIdRecordInterceptorTest.java`: zkopírovat `order-service/src/test/java/cz/demo/eda/order/support/CorrelationIdRecordInterceptorTest.java`, změnit jen balíček na `cz.demo.eda.process.support`.

`config/ProcessPropertiesTest.java`:

```java
package cz.demo.eda.process.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThat;

class ProcessPropertiesTest {

    @Test
    @DisplayName("Přijme kladné TTL zprávy")
    void should_keepTtl_whenPositive() {
        assertThat(new ProcessProperties(Duration.ofMinutes(1)).messageTtl()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    @DisplayName("Odmítne nulové TTL – zpráva, která předběhne proces, by se ztratila")
    void should_reject_whenTtlIsZero() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ProcessProperties(Duration.ZERO));
    }

    @Test
    @DisplayName("Odmítne chybějící TTL")
    void should_reject_whenTtlIsNull() {
        assertThatIllegalArgumentException().isThrownBy(() -> new ProcessProperties(null));
    }
}
```

Run: `cd order-process && mvn -q test`
Expected: FAIL – compilation errors (třídy neexistují).

- [ ] **Step 3: Implementace**

`OrderProcessApplication.java`:

```java
package cz.demo.eda.process;

import io.camunda.client.annotation.Deployment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Vstupní bod order-process; při startu nasadí BPMN modely z classpath do Zeebe. */
@SpringBootApplication
@ConfigurationPropertiesScan
@Deployment(resources = "classpath*:bpmn/*.bpmn")
public class OrderProcessApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderProcessApplication.class, args);
    }
}
```

`config/AppConfig.java`: kopie z order-service, balíček `cz.demo.eda.process.config`.

`config/EdaKafkaProperties.java`: kopie z order-service, balíček `cz.demo.eda.process.config`.

`config/ProcessProperties.java`:

```java
package cz.demo.eda.process.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Nastavení napojení na proces.
 *
 * @param messageTtl jak dlouho Zeebe drží publikovanou zprávu, než ji někdo zkoreluje – pokryje
 *                   výsledek platby, který předběhne proces, a po tuto dobu deduplikuje stejné messageId
 */
@ConfigurationProperties("eda.process")
public record ProcessProperties(@DefaultValue("1m") Duration messageTtl) {

    public ProcessProperties {
        if (messageTtl == null || messageTtl.isNegative() || messageTtl.isZero()) {
            throw new IllegalArgumentException("eda.process.message-ttl must be positive");
        }
    }
}
```

`config/KafkaConfig.java`: kopie `order-service/.../config/KafkaConfig.java` (balíček `cz.demo.eda.process.config`, importy `cz.demo.eda.process.support.*`) s těmito změnami:

```java
/** Topicy, retry s backoffem a dead letter topicy pro konzumenty orders.created a payments.result. */
@Configuration(proxyBeanMethods = false)
public class KafkaConfig {
    ...
    @Bean
    KafkaAdmin.NewTopics edaTopics(EdaKafkaProperties properties) {
        EdaKafkaProperties.Topics settings = properties.topics();
        return new KafkaAdmin.NewTopics(
                topic(Topics.ORDERS_CREATED, settings),
                topic(Topics.PAYMENTS_COMMANDS, settings),
                topic(Topics.PAYMENTS_RESULT, settings),
                topic(Topics.ORDERS_COMMANDS, settings),
                // DLT má stejný počet partitions, aby recoverer mohl zachovat číslo partition.
                topic(Topics.dltOf(Topics.ORDERS_CREATED), settings),
                topic(Topics.dltOf(Topics.PAYMENTS_RESULT), settings));
    }
```

a v `kafkaErrorHandler` odstranit parametr `MessagingMetrics metrics`, řádek `metrics.deadLetterCounter(...)` a `metrics.deadLettered(...)` (order-process business metriky nemá – YAGNI); `setCommitRecovered` neuvádět (AckMode.RECORD).

`support/Topics.java`:

```java
package cz.demo.eda.process.support;

/** Názvy Kafka topiců – součást kontraktu s ostatními službami, musí odpovídat jejich konfiguraci. */
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String PAYMENTS_COMMANDS = "payments.commands";
    public static final String PAYMENTS_RESULT = "payments.result";
    public static final String ORDERS_COMMANDS = "orders.commands";
    /** Suffix dead letter topiců; resolver v konzumentech jej používá explicitně. */
    public static final String DLT_SUFFIX = ".DLT";

    private Topics() {
    }

    /** Vrátí název dead letter topicu pro zadaný topic. */
    public static String dltOf(String topic) {
        return topic + DLT_SUFFIX;
    }
}
```

`support/Tracing.java`, `support/CorrelationIdRecordInterceptor.java`: kopie z order-service, balíček `cz.demo.eda.process.support`.

`event/DomainEvent.java`: kopie z order-service, balíček `cz.demo.eda.process.event`, JavaDoc „Společná metadata všech zpráv; eventId slouží konzumentům k deduplikaci.“

`event/OrderCreated.java` (jen příjem – bez `of`):

```java
package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Událost order-service o založení objednávky – spouští proces. */
public record OrderCreated(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency) implements DomainEvent {

    public OrderCreated {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
    }
}
```

`event/PaymentResult.java`:

```java
package cz.demo.eda.process.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Výsledek platby od payment-service; podtyp určuje pole {@code type}. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PaymentCompleted.class, name = "PaymentCompleted"),
        @JsonSubTypes.Type(value = PaymentFailed.class, name = "PaymentFailed")
})
public sealed interface PaymentResult extends DomainEvent permits PaymentCompleted, PaymentFailed {
}
```

`event/PaymentCompleted.java`:

```java
package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba proběhla úspěšně. */
public record PaymentCompleted(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String paymentId,
        BigDecimal amount) implements PaymentResult {

    public PaymentCompleted {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
    }
}
```

`event/PaymentFailed.java`:

```java
package cz.demo.eda.process.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Platba byla zamítnuta (business výsledek). */
public record PaymentFailed(
        UUID eventId,
        Instant timestamp,
        String correlationId,
        String orderId,
        String reason) implements PaymentResult {

    public PaymentFailed {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(orderId, "orderId");
    }
}
```

`event/ProcessPayment.java`:

```java
package cz.demo.eda.process.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Příkaz pro payment-service: proveď platbu za objednávku. */
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
}
```

`event/OrderCommand.java`, `event/ConfirmOrder.java`, `event/CancelOrder.java`: stejné jako v Task 2 (balíček `cz.demo.eda.process.event`), **bez** statických metod `of` – order-process tvoří příkazy vždy s deterministickým eventId (Task 6). JavaDoc `OrderCommand`: „Příkaz pro order-service. Oba podtypy sdílí jeden topic, proto nesou v JSON diskriminátor {@code type}.“

`src/main/resources/application.yml`:

```yaml
spring:
  application:
    name: order-process
  # Virtuální vlákna vypnutá ze stejného důvodu jako v ostatních službách (pinning Kafka consumeru na Java 21).
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    admin:
      fail-fast: true
    producer:
      acks: all
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
      properties:
        enable.idempotence: true
        spring.json.add.type.headers: false
    consumer:
      group-id: order-process
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.key.delegate.class: org.apache.kafka.common.serialization.StringDeserializer
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
        # Výchozí typ nastavuje každý listener sám (dva topicy, dva typy).
        spring.json.use.type.headers: false
        spring.json.trusted.packages: cz.demo.eda.process.event
    listener:
      # Offset se commitne až po úspěšném publishMessage do Zeebe.
      ack-mode: record
      concurrency: 3

camunda:
  client:
    mode: self-managed
    auth:
      method: none
    rest-address: ${CAMUNDA_REST_ADDRESS:http://localhost:8088}
    grpc-address: ${CAMUNDA_GRPC_ADDRESS:http://localhost:26500}
    # gRPC kvůli jednoznačnému kódu ALREADY_EXISTS při duplicitním messageId (ProcessGateway).
    prefer-rest-over-grpc: false

eda:
  process:
    message-ttl: 1m
  kafka:
    topics:
      partitions: 3
      replicas: 1
    retry:
      max-retries: 3
      initial-interval: 500ms
      multiplier: 2.0
      max-interval: 5s

server:
  shutdown: graceful

management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
  endpoint:
    health:
      probes:
        enabled: true
  metrics:
    tags:
      application: ${spring.application.name}

logging:
  structured:
    format:
      console: ecs
    ecs:
      service:
        name: ${spring.application.name}
```

`Dockerfile`, `.dockerignore`: zkopírovat z `order-service/` beze změny.

- [ ] **Step 4: Testy**

Run: `cd order-process && mvn -q test`
Expected: PASS (zatím jen unit testy; kontext se nespouští).

---

