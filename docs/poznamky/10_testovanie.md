# 10 – Testovanie

## Čo sa naučíš

- Ako sú testy rozvrstvené: **unit** (Mockito), **repository** (`@DataJpaTest` proti PostgreSQL v Testcontainers), **integračné** (Kafka + Zeebe v kontajneroch)
- Čo je **Camunda Process Test** a ako `OrderProcessIntegrationTest` testuje celý most Kafka ↔ Zeebe
- Ako testy spustiť a koľko trvajú

---

## Teória v skratke

### 1. Testovacia pyramída v deme

| Vrstva | Čo testuje | Nástroje | Rýchlosť | Príklad |
|---|---|---|---|---|
| Unit | jedna trieda, závislosti zamockované | JUnit 5, Mockito, AssertJ | ms | `ProcessGatewayTest`, `CommandPublisherTest`, `InboxServiceTest` |
| Repository | JPA dotazy, zámky, JSONB proti **skutočnému** PostgreSQL | `@DataJpaTest` + Testcontainers `postgres:18.6-alpine` | sekundy | `InboxRepositoryTest`, `OutboxRepositoryTest` |
| Integračný | celý tok cez skutočnú Kafku (a Zeebe) | `@SpringBootTest` + Testcontainers `apache/kafka:4.3.1` + Camunda Process Test | desiatky sekúnd | `OrderServiceIntegrationTest`, `OrderProcessIntegrationTest` |

Prečo repository testy proti skutočnej DB, nie H2? Lebo testujeme veci, ktoré H2 nevie alebo vie inak: `FOR UPDATE SKIP LOCKED`, `JSONB`, čiastočné indexy, Flyway migrácie. Test proti inej DB by prešiel a produkcia by spadla.

### 2. Testcontainers

Testcontainers spustí pre test Docker kontajner (PostgreSQL, Kafka) a po teste ho zmaže. Spring Boot `@ServiceConnection` automaticky nastaví `spring.datasource.*` / `spring.kafka.bootstrap-servers` na adresu kontajnera:

```java
@Bean
@ServiceConnection
KafkaContainer kafka() {
    return new KafkaContainer("apache/kafka:4.3.1");   // rovnaká verzia ako v Kubernetes
}
```

Kontajner `testcontainers/ryuk` stráži upratanie, aj keď test spadne.

### 3. Camunda Process Test (CPT)

`camunda-process-test-spring` spustí **Camundu 8.10.2 v Testcontaineri** a napojí na ňu `CamundaClient` aplikácie. Anotácia `@CamundaSpringProcessTest`:

- pred testom spustí (alebo znovu použije) runtime Camundy,
- nasadí BPMN (cez `@Deployment` aplikácie),
- po teste vyčistí stav,
- sprístupní `CamundaAssert` – asserty nad inštanciami: `isCompleted()`, `isActive()`, `hasCompletedElements(...)`, `isWaitingForMessage(...)`, `hasVariable(...)`.

`OrderProcessIntegrationTest` používa **produkčné** listenery aj workery. Testuje teda presne kód, ktorý beží v clustri, len Kafka a Zeebe sú v kontajneroch.

### 4. Čo pokrývajú integračné testy `order-process`

| Test | Scenár | Kapitola |
|---|---|---|
| `should_confirmOrder_whenPaymentCompleted` | šťastná cesta až po `ConfirmOrder`, `paymentStatus = COMPLETED` | 07, 08 |
| `should_cancelOrder_whenPaymentFailed` | zamietnutie → `CancelOrder` s dôvodom | 07 |
| `should_waitForPaymentResult_whenPaymentRequested` | po `ProcessPayment` inštancia čaká na správu | 07 |
| `should_startSingleInstance_whenOrderCreatedDeliveredTwice` | duplicitný `OrderCreated` → 1 inštancia, 1 `ProcessPayment`, nič v DLT | 08 |
| `should_confirmOrder_whenPaymentResultArrivesBeforeProcessWaits` | predbehnutá správa → zkoreluje sa | 08, 09 |
| `should_confirmOnce_whenPaymentResultDeliveredTwice` | duplicitný výsledok → 1 `ConfirmOrder` | 08 |
| `should_notDeadLetter_whenPaymentResultForUnknownOrder` | výsledok bez inštancie → TTL, nie DLT | 08 |
| `should_moveToDlt_whenOrderCreatedUnreadable` | `not json` → `orders.created.DLT` | 04, 09 |

Niektoré z týchto scenárov (predbehnutá správa) sa naživo vyvolávajú ťažko – test ich overuje deterministicky.

---

## Krok po kroku

Predpoklad: beží Docker Desktop (Testcontainers ho potrebujú aj mimo Kubernetes), JDK 21, Maven 3.9+. Spúšťaj z koreňa repa. Testy bežia popri stacku v Kubernetes bez konfliktu (vlastné kontajnery, náhodné porty) – pri písaní bežal stack súčasne.

### 1. Rýchle unit testy

```powershell
mvn -B -f order-process\pom.xml test "-Dtest=ProcessGatewayTest,CommandPublisherTest" 2>$null | Select-String 'Tests run:|BUILD|Total time' | ForEach-Object { $_.Line }
```

```
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.637 s -- in cz.demo.eda.process.messaging.CommandPublisherTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.941 s -- in cz.demo.eda.process.process.ProcessGatewayTest
[INFO] Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
[INFO] Total time:  6.934 s
```

`-Dtest` musí byť v úvodzovkách – PowerShell inak čiarku chápe ako oddeľovač poľa.

### 2. Všetky testy `order-process` (vrátane Camunda Process Test)

```powershell
mvn -B -f order-process\pom.xml test 2>$null | Select-String 'Tests run:|BUILD|Total time' | ForEach-Object { $_.Line }
```

```
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.130 s -- in cz.demo.eda.process.config.ProcessPropertiesTest
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.330 s -- in cz.demo.eda.process.event.EventSerializationTest
[INFO] Tests run: 5, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.417 s -- in cz.demo.eda.process.messaging.CommandPublisherTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.155 s -- in cz.demo.eda.process.messaging.OrderCreatedListenerTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.069 s -- in cz.demo.eda.process.messaging.PaymentResultListenerTest
[INFO] Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 82.46 s -- in cz.demo.eda.process.OrderProcessIntegrationTest
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 1.381 s -- in cz.demo.eda.process.process.ProcessGatewayTest
...
[INFO] Tests run: 42, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
[INFO] Total time:  01:32 min
```

(Pri písaní spustené cez Git Bash s identickými argumentmi `mvn -B -f order-process/pom.xml test`, výstup Mavenu je rovnaký.)

`OrderProcessIntegrationTest` trvá ~80 s – väčšina je štart Camundy v kontajneri. V logu testu uvidíš, ktoré kontajnery Testcontainers spustili:

```
Creating container for image: testcontainers/ryuk:0.14.0
Creating container for image: apache/kafka:4.3.1
Container apache/kafka:4.3.1 started in PT4.3577989S
Creating container for image: camunda/camunda:8.10.2
Creating container for image: testcontainers/sshd:1.3.0
```

(`sshd` je pomocný kontajner Testcontainers na sprístupnenie portov hostiteľa kontajnerom.)

### 3. Doménové služby

```powershell
mvn -B -f order-service\pom.xml test 2>$null | Select-String 'Tests run: \d+, F.*$' | Select-Object -Last 1 | ForEach-Object { $_.Line }
mvn -B -f payment-service\pom.xml test 2>$null | Select-String 'Tests run: \d+, F.*$' | Select-Object -Last 1 | ForEach-Object { $_.Line }
```

```
[INFO] Tests run: 96, Failures: 0, Errors: 0, Skipped: 0
[INFO] Tests run: 94, Failures: 0, Errors: 0, Skipped: 0
```

order-service ~40 s, payment-service ~49 s (pri písaní spustené cez Git Bash). Spolu s `order-process` **232 testov, 0 zlyhaní** – zhoduje sa s poslednou verifikáciou v `progress.md` (96 + 94 + 42).

Všetko naraz cez agregátor (README): `mvn verify` v koreni. Toto som pri písaní nespúšťal – spúšťal som tri projekty zvlášť.

---

## Kód z repa

| Súbor | Na čo sa pozrieť |
|---|---|
| [`order-process/src/test/.../OrderProcessIntegrationTest.java`](../../order-process/src/test/java/cz/demo/eda/process/OrderProcessIntegrationTest.java) | `@SpringBootTest` + `@CamundaSpringProcessTest` + `@Import(KafkaTestcontainer.class)`; skrátené Kafka retry (`50ms`/`100ms`), `CamundaAssert.setAssertionTimeout(30 s)` |
| [`order-process/src/test/.../KafkaTestSupport.java`](../../order-process/src/test/java/cz/demo/eda/process/KafkaTestSupport.java) | posielanie „surového“ JSON a čakanie na záznam podľa kľúča – test hovorí s aplikáciou len cez Kafku |
| [`order-process/src/test/.../process/ProcessGatewayTest.java`](../../order-process/src/test/java/cz/demo/eda/process/process/ProcessGatewayTest.java) | Mockito s `RETURNS_DEEP_STUBS` na fluent API `CamundaClient`; `ALREADY_EXISTS` → `false`, `UNAVAILABLE` → výnimka, `null` messageId → NPE |
| [`order-process/src/test/.../messaging/CommandPublisherTest.java`](../../order-process/src/test/java/cz/demo/eda/process/messaging/CommandPublisherTest.java) | stabilné `commandId` pre rovnaký `jobKey`, hraničné `jobKey` 0 a `Long.MAX_VALUE` |
| [`order-service/src/test/.../inbox/InboxRepositoryTest.java`](../../order-service/src/test/java/cz/demo/eda/order/inbox/InboxRepositoryTest.java) | `@DataJpaTest` + `@AutoConfigureTestDatabase(replace = NONE)` + `PostgresTestcontainer` |
| [`order-service/src/test/.../OrderServiceIntegrationTest.java`](../../order-service/src/test/java/cz/demo/eda/order/OrderServiceIntegrationTest.java) | objednávka → outbox → `orders.created`, `ConfirmOrder` → inbox → `PAID` práve raz; nečitateľný príkaz → DLT |

Ukážka assertu nad procesom:

```java
CamundaAssert.assertThatProcessInstance(ProcessInstanceSelectors.byProcessId(ProcessMessages.PROCESS_ID))
        .isCompleted()
        .hasCompletedElements("request_payment", "payment_result", "confirm_order", "order_confirmed")
        .hasVariable("paymentStatus", "COMPLETED");
```

Asserty CPT **čakajú** (do nastaveného timeoutu), kým podmienka začne platiť – proces beží asynchrónne, takže `Thread.sleep` netreba.

Konvencie testov v repe: názvy `should_<čo>_when<podmienka>`, `@DisplayName` s popisom (po česky), Mockito pre závislosti, Testcontainers pre DB a Kafku.

---

## Bez Camundy by to vyzeralo takto

V choreografii (`ba78d05`, 187 testov: order 94, payment 93) končil end-to-end test pri jednej službe: „pošli `OrderCreated` do Kafky, over `PaymentCompleted` na výstupe“. **Celý tok** (objednávka → platba → potvrdenie) sa dal overiť len v nasadenom clustri.

S orchestrátorom je tok v jednom mieste, a preto ho jeden test (`OrderProcessIntegrationTest`) overí celý – s doménovými službami „nahradenými“ správami v Kafke. Cena: ďalší ťažký kontajner (Camunda) a ~80 s navyše.

---

## Časté chyby

| Príznak | Príčina | Riešenie |
|---|---|---|
| `Could not find a valid Docker environment` | Nebeží Docker Desktop | Spusti Docker; testy potrebujú Docker aj bez Kubernetes |
| `OrderProcessIntegrationTest` padá na timeout pri prvom spustení | Sťahuje sa image `camunda/camunda:8.10.2` (stovky MB) | Spusti znova, alebo vopred `docker pull camunda/camunda:8.10.2` |
| CPT assert zlyhá „expected completed but was active“ | Predvolený timeout asertov (10 s) nestačí na štart workerov a prvý poll Kafky | V teste je `CamundaAssert.setAssertionTimeout(30 s)` |
| `-Dtest=A,B` v PowerShelli spustí všetko alebo hlási chybu | Čiarka v PS bez úvodzoviek | `"-Dtest=A,B"` |
| Repository test prejde lokálne s H2, padne v CI | Iná DB nevie `SKIP LOCKED`/`JSONB` | Demo používa len PostgreSQL v Testcontainers |
| Málo RAM, kontajnery testov padajú | Stack v Kubernetes + Camunda v teste naraz | Pri písaní to s ~10 GiB pre Docker VM stačilo; inak dočasne zastav stack |

---

## Otázky na zopakovanie

**Prečo repository testy bežia proti PostgreSQL a nie H2?**
Testujú `SKIP LOCKED`, `JSONB`, Flyway migrácie – veci, ktoré H2 nemá alebo má inak.

**Čo robí `@CamundaSpringProcessTest`?**
Spustí Camundu v Testcontaineri, napojí na ňu klienta aplikácie, nasadí BPMN a sprístupní `CamundaAssert` na overovanie stavu inštancií.

**Prečo integračný test `order-process` posiela do Kafky „surový“ JSON a nie Java objekty?**
Lebo kontraktom je JSON (služby nezdieľajú triedy). Test tak overí aj deserializáciu tak, ako ju uvidí produkcia.

**Ktorý scenár sa ľahšie overí testom ako naživo?**
Predbehnutá správa (`PaymentResult` pred `OrderCreated`) – naživo by si musel zaseknúť worker presne medzi odoslaním a `completeJob`.

## Vyskúšaj si

1. Spusti len `OrderProcessIntegrationTest` (`"-Dtest=OrderProcessIntegrationTest"`) a počas behu v druhom okne `docker ps --format "{{.Image}}"`. **Očakávanie:** okrem kontajnerov Kubernetes uvidíš `apache/kafka:4.3.1`, `camunda/camunda:8.10.2` a `testcontainers/ryuk`.
2. Prečítaj `should_startSingleInstance_whenOrderCreatedDeliveredTwice` a vysvetli, ktorý riadok produkčného kódu zaručuje, že vznikne len jedna inštancia. **Očakávanie:** `.messageId(messageId.toString())` v `ProcessGateway.publish` + spracovanie `ALREADY_EXISTS`.

---

**Ďalej:** [11 – Observabilita](11_observabilita.md)
