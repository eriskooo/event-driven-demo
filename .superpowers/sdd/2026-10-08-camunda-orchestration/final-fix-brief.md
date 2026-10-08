# Final fix wave – order orchestration (uncommitted working tree; NO git writes)

Repo: C:\projekty\tmp\event-driven-demo. Modules: order-service, payment-service, order-process. Spec: docs/superpowers/specs/2026-10-08-camunda-orchestration-design.md.
Rules: no `var`; Czech JavaDoc on every public method AND public constructor; comments explain WHY; English identifiers; JUnit 5 + Mockito, `should_x_whenY`, Czech @DisplayName; SLF4J; never empty catch; alphabetical imports. Services share no code.

## Fixes (all required)

F1 (Important) Message TTL shorter than job timeout → stuck instance.
- Raise `eda.process.message-ttl` default to `1h` in order-process/src/main/resources/application.yml and in `ProcessProperties` `@DefaultValue`. Update its JavaDoc: TTL must exceed job timeout (5 min default) + time to resolve an incident, otherwise a PaymentResult that arrived while the instance still sat on request_payment expires and the instance waits forever; it is also the dedup window for OrderCreated.
- Adjust any test that asserts the default (ProcessPropertiesTest uses explicit values — keep).
- Update the spec table row? NO (spec is the user's doc; leave it). Update README idempotence/known-behaviour text and config table to say 1h and why.

F2 Positive "overtake" e2e test (order-process OrderProcessIntegrationTest): send PaymentResult (PaymentCompleted, paymentId p-1) for a new orderId FIRST, then OrderCreated for the same orderId, then assert ConfirmOrder arrives on orders.commands and the process instance completes. Name: should_confirmOrder_whenPaymentResultArrivesBeforeProcessWaits, Czech DisplayName.

F3 Duplicate tests strengthen (OrderProcessIntegrationTest):
- should_startSingleInstance_whenOrderCreatedDeliveredTwice: additionally assert `orders.created.DLT` has no record for that orderId (proves ALREADY_EXISTS is swallowed rather than erroring).
- should_moveToDlt_whenOrderCreatedUnreadable: DisplayName claims "proces nespustí" — rephrase DisplayName to only what is asserted ("Nečitelná zpráva v orders.created skončí v DLT").

F4 correlationId in worker logs: in each of RequestPaymentWorker, ConfirmOrderWorker, CancelOrderWorker put `variables.correlationId()` into MDC (key `Tracing.CORRELATION_ID_MDC_KEY`) for the duration of the handler (try/finally remove; skip put when null). Keep it DRY: a small package-private/helper is fine if it avoids triplication, but keep it simple. Unit test: one test per worker is not needed — add one test asserting MDC is set during publisher.send (e.g. in RequestPaymentWorkerTest capture MDC value inside a doAnswer) and cleared after.

F5 Czech JavaDoc on public constructors: ProcessProperties compact ctor, CommandPublisher, RequestPaymentWorker, ConfirmOrderWorker, CancelOrderWorker (check also other new public ctors in order-process: ProcessGateway, listeners — add if missing).

F6 CommandPublisher: one-line Czech comment at the timeout catch explaining the future is left running and a late delivery is harmless because the retry reuses the same commandId and the receiver's inbox deduplicates.

F7 Tests in domain services:
- payment-service ProcessPaymentListenerTest: DisplayName "Objednávku uloží do inboxu…" → "Příkaz k platbě uloží do inboxu…"; helper `order()` → `command()`.
- order-service OrderServiceTest: rename tests still saying "výsledkem platby"/"Výsledek platby" to command wording (e.g. should_keepFinalState_whenSecondCommandArrives, "Finální stav se dalším příkazem nezmění", "Příkaz pro neznámou objednávku vrátí prázdno").
- order-service OrderCommandHandlerTest: add happy-path test for ConfirmOrder (verifies `type` discriminator → ConfirmOrder → orderService.applyCommand(confirm)).
- order-process PaymentResultListenerTest: add should_propagate_whenGatewayFails.

F8 README.md fixes:
- Topic table: `*.DLT` partitions = 3 (same as source topic; KafkaConfig creates DLT with settings.partitions), not 1.
- Sentence "Topic zakládá služba, která z něj čte" → each service creates topics it produces or consumes plus DLTs of its consumers.
- Config table: `eda.kafka.retry.*` for order-process = retry of publishMessage to Zeebe (no inbox there); add `CAMUNDA_GRPC_ADDRESS` (gRPC is the client protocol actually used, `prefer-rest-over-grpc: false`).
- "Známé chování dema": add — Zeebe unavailable longer than Kafka retry (~3.5 s) → OrderCreated goes to orders.created.DLT (nobody reads it), order stays CREATED/PENDING_PAYMENT without instance; restart/redrive manually.
- "Co bylo ověřeno": replace "k8s deploy not run" with the real verification done on 2026-10-08 (docker-desktop): teardown + build-images + deploy OK, 6/6 pods Ready; 10 orders → 9 PAID, 1 PAYMENT_FAILED (failure rate 0.2); amount 666 → PENDING_PAYMENT, instance ACTIVE on `payment_result` in Camunda, payment-service logged record from payments.commands.DLT; ERROR logs only the two expected poison lines; Camunda search API shows 10 COMPLETED + 1 ACTIVE; Camunda memory peak ~550 MB of 1536Mi limit; Prometheus endpoint on 9600 responds.
- Postgres port-forward: local port 5432 may be taken by a host PostgreSQL; document using `kubectl -n eda-demo port-forward svc/postgres 15432:5432` and JDBC `jdbc:postgresql://localhost:15432/eda` as alternative (do not change scripts).

## Verification
Run `mvn -q test` in each changed module (order-service, payment-service, order-process — Docker is running; order-process e2e takes ~2 min). Report counts.
