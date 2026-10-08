# Final fix report (F1-F8)

Commits created: none (per user rule). No kubectl/deploy run.

- F1: message-ttl default 1h in application.yml (+ comment) and ProcessProperties @DefaultValue; JavaDoc explains TTL > job timeout + incident time, dedup window. README config table + known-behaviour row updated. ProcessPropertiesTest unchanged (explicit values).
- F2: OrderProcessIntegrationTest.should_confirmOrder_whenPaymentResultArrivesBeforeProcessWaits (PaymentResult first, then OrderCreated; asserts ConfirmOrder p-1 and completed instance). Passed.
- F3: duplicate test also asserts orders.created.DLT empty for orderId; DisplayName of unreadable test reduced to "Nečitelná zpráva v orders.created skončí v DLT".
- F4: new package-private worker/CorrelationScope (AutoCloseable, MDC put/remove, skips null); used via try-with-resources in all three workers. Test RequestPaymentWorkerTest.should_setMdcDuringSend_whenCorrelationIdPresent (MDC captured inside doAnswer, cleared afterwards).
- F5: Czech JavaDoc added on ProcessProperties compact ctor, CommandPublisher, 3 worker ctors. ProcessGateway, listeners, CommandPublishException already had it.
- F6: Czech comment at CommandPublisher timeout/execution catch.
- F7: ProcessPaymentListenerTest DisplayName + helper command(); OrderServiceTest renamed (should_keepFinalState_whenSecondCommandArrives, new DisplayNames); OrderCommandHandlerTest.should_applyConfirmOrder_whenTypeIsConfirmOrder; PaymentResultListenerTest.should_propagate_whenGatewayFails.
- F8: README: DLT partitions 3; topic-creation sentence; retry rows split (order-process = publishMessage retry); CAMUNDA_GRPC_ADDRESS row; known behaviour (Zeebe down > ~3.5 s -> orders.created.DLT); "Co bylo ověřeno" replaced with real 2026-10-08 k8s verification; Postgres 15432 port-forward alternative.

## Tests (`mvn -q test` per module, all green)
- order-service: 102 run, 0 fail
- payment-service: 100 run, 0 fail
- order-process: 42 run, 0 fail (incl. e2e with Docker)
(counts summed from target/surefire-reports)

## Deviations / concerns
- None of substance. README "Co bylo ověřeno" k8s figures were taken verbatim from the brief, not re-verified.
- Surefire count aggregation may include stale report files from earlier runs.
