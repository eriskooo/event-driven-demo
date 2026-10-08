# Task 1 report

Status: DONE_WITH_CONCERNS (container tests not run, Docker daemon not running)
Commits created: none (per user rule)

## Implemented
Per brief: new ProcessPayment record, ProcessPaymentListener/Handler (old OrderCreated* deleted, tests renamed by create+delete),
Topics.PAYMENTS_COMMANDS, KafkaConfig, DeadLetterListener (id process-payment-dlt-listener), Payment/PaymentService/PaymentSimulator types,
application.yml default type, pom description, PaymentService JavaDoc. Added should_ignoreOrdersCreated_whenOrchestrated to PaymentServiceIntegrationTest.

## TDD
RED: `mvn -q test-compile` -> `cannot find symbol` for ProcessPayment in tests.
GREEN: `mvn test -Dtest='!*IntegrationTest,!*RepositoryTest' -Dsurefire.failIfNoSpecifiedTests=false` -> Tests run: 77, Failures: 0, Errors: 0, BUILD SUCCESS.

## Not run
`docker info`: server unreachable (dockerDesktopLinuxEngine pipe missing). Integration/repository (Testcontainers) tests NOT run,
incl. the new should_ignoreOrdersCreated_whenOrchestrated (compiles only).

## Leftovers check
grep for OrderCreated|ORDERS_CREATED|order-created in payment-service/src/main and pom: none.
Remaining "orders.created" strings in tests: only the new integration test plus arbitrary topic literals in Inbox*/Outbox{Repository,Service}/InboxService tests
(not in the brief's list; harmless fixtures, left unchanged).

## Self-review
Diff mechanical and matches brief; no var, Czech JavaDoc kept. Concern: container tests unverified.

## Update: full run with Docker
`cd payment-service && mvn test` -> Tests run: 94, Failures: 0, Errors: 0, BUILD SUCCESS
(incl. PaymentServiceIntegrationTest 4 tests with should_ignoreOrdersCreated_whenOrchestrated, and all *RepositoryTest).
Status now: DONE. Commits created: none (per user rule).
