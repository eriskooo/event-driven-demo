# Task 7 report: BPMN model and end-to-end process test

**Status:** DONE
**Commits created:** none (per user rule)

## Implementation
- `order-process/src/main/resources/bpmn/order-fulfillment.bpmn`: copied byte-for-byte from the brief (lines 209–335 extracted with `sed`, so there are no transcription differences). The element ids, job types (`request-payment`, `confirm-order`, `cancel-order`), message names (`OrderCreated`, `PaymentResult`, correlationKey `=orderId`) and the gateway condition all match the brief and `ProcessMessages`.
- `order-process/src/test/java/cz/demo/eda/process/OrderProcessIntegrationTest.java`: copied verbatim from the brief. It has all 7 scenarios. The imports and API were not changed: `io.camunda.process.test.api.CamundaAssert`, `CamundaSpringProcessTest` and `io.camunda.process.test.api.assertions.ProcessInstanceSelectors` all compile against 8.10.2 as written.
- `order-process/src/test/java/cz/demo/eda/process/KafkaTestcontainer.java` and `KafkaTestSupport.java`: copied from order-service with only the package line changed (`sed`).

## TDD evidence
**RED** (test and test support in place, no BPMN yet): `mvn -q test -Dtest=OrderProcessIntegrationTest`
```
[ERROR] Tests run: 7, Failures: 0, Errors: 7, Skipped: 0, Time elapsed: 70.48 s <<< FAILURE!
[ERROR]   OrderProcessIntegrationTest.should_cancelOrder_whenPaymentFailed » IllegalArgument No resources found to deploy
... (same for all 7)
java.lang.IllegalArgumentException: No resources found to deploy
	at io.camunda.client.spring.annotation.processor.DeploymentAnnotationProcessor.deploy(DeploymentAnnotationProcessor.java:121)
```
The test fails for the reason the brief expects (no BPMN to deploy). The cause shows up one step earlier than the brief predicted: `@Deployment` fails in the `CamundaProcessTestExecutionListener.beforeTestMethod` callback, before `awaitCommand` can time out. This also shows that the Spring context starts: the Camunda client auto-config works on Boot 4.1, Jackson 2 and Jackson 3 coexist, and the Kafka listeners are wired.

**GREEN** (after adding the BPMN): same command, exit 0. From `target/surefire-reports`:
```
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 80.63 s -- in cz.demo.eda.process.OrderProcessIntegrationTest
should_cancelOrder_whenPaymentFailed 7.833s
should_notDeadLetter_whenPaymentResultForUnknownOrder 8.635s
should_confirmOnce_whenPaymentResultDeliveredTwice 10.566s
should_moveToDlt_whenOrderCreatedUnreadable 1.998s
should_waitForPaymentResult_whenPaymentRequested 2.341s
should_confirmOrder_whenPaymentCompleted 3.143s
should_startSingleInstance_whenOrderCreatedDeliveredTwice 6.956s
```

## Test results
- `cd order-process && mvn -q test`: exit 0. 39 tests: 7 integration and 32 unit (ProcessProperties 3, EventSerialization 5, CommandPublisher 5, OrderCreatedListener 4, PaymentResultListener 3, ProcessGateway 4, CorrelationIdRecordInterceptor 2, Topics 2, CancelOrderWorker 1, ConfirmOrderWorker 1, RequestPaymentWorker 2). No failures.
- Repo root `mvn -q test`: exit 0. order-service 101 tests, payment-service 100, order-process 39. No failures, errors or skips.

## Problems hit
None. No debugging was needed after the BPMN was added. The log confirms that the risky points work at runtime:
- **gRPC ALREADY_EXISTS deduplication works:** `Duplicate message OrderCreated for o-8763… skipped` and `Duplicate message PaymentResult for o-15d8… skipped`. Both come from `ProcessGateway`, so the duplicate test scenarios pass for the intended reason.
- **BigDecimal round-trip:** `amount` 10.50 goes from Kafka into Zeebe variables, then through `OrderVariables` into `ProcessPayment`. The `isEqualByComparingTo("10.50")` assertion passes.
- **DLT:** `Record orders.created-1@2 moved to orders.created.DLT after retries` (ERROR from KafkaConfig). This is expected and belongs to the "unreadable message" scenario.
- **Isolation between tests:** the container logs `Cluster purge detected…` before each test. Camunda Process Test purges data between tests, so `byProcessId` sees only the current test's instance. The `@DirtiesContext` fallback was **not** used.

## Production changes
None. Tasks 3–6 classes, `application.yml` and `pom.xml` are unchanged.

## Deviations from the brief
None in the content. The only deviation is the exact point where the RED failure surfaces (described above).

## Concerns / notes
- Log noise, harmless:
  - `CamundaCallCredentials` WARN "security level does not guarantee that the credentials will be confidential" appears 14 times. It comes from plaintext gRPC with `auth.method: none`.
  - The Camunda container logs one ERROR "Failed to take a snapshot for StreamProcessor-1" and many UNHEALTHY/recovered messages for the partition around the per-test cluster purge. These are internal to the test runtime and do not affect results.
- The integration test takes about 80 s, mostly container startup: Kafka plus the Camunda container.

## Files
- Created: `order-process/src/main/resources/bpmn/order-fulfillment.bpmn`
- Created: `order-process/src/test/java/cz/demo/eda/process/OrderProcessIntegrationTest.java`
- Created: `order-process/src/test/java/cz/demo/eda/process/KafkaTestcontainer.java`
- Created: `order-process/src/test/java/cz/demo/eda/process/KafkaTestSupport.java`
