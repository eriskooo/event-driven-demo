# Task 2 report: order-service přijímá ConfirmOrder / CancelOrder z orders.commands

Status: DONE. Commits created: none (per user rule).

## Implemented
Exactly per brief: new OrderCommand (sealed, @JsonTypeInfo "type"), ConfirmOrder, CancelOrder; OrderCommandListener/Handler;
OrderService.applyCommand replaces applyPaymentResult; Topics.ORDERS_COMMANDS replaces PAYMENTS_RESULT; KafkaConfig (topics, DLT, DLT counter, JavaDoc);
application.yml default type = OrderCommand; pom description. Deleted PaymentResult/Completed/Failed, PaymentResultListener/Handler and their tests.

## Tests / TDD
- RED: `mvn -q test` after test edits -> `cannot find symbol` (ConfirmOrder, CancelOrder, OrderCommand...) in OrderServiceIntegrationTest, OrderServiceTest, OrderCommandHandlerTest.
- GREEN: `cd order-service && mvn -q test` -> exit 0 (incl. Testcontainers integration test).
- `grep -rn "PaymentResult\|PaymentCompleted\|PaymentFailed\|PAYMENTS_RESULT" order-service/src` -> no matches.

## Files
Main: event/{OrderCommand,ConfirmOrder,CancelOrder}.java (new); messaging/{OrderCommandListener,OrderCommandHandler}.java (new); domain/OrderService.java, support/Topics.java, config/KafkaConfig.java, resources/application.yml, pom.xml (modified); 5 Payment* classes deleted.
Tests: messaging/OrderCommandListenerTest, OrderCommandHandlerTest (new); EventSerializationTest, OrderServiceTest, InboxRepositoryTest, OrderServiceIntegrationTest (modified); PaymentResultListenerTest, PaymentResultHandlerTest deleted.

## Self-review
- BigDecimal import removed from listener test and integration test; kept in OrderServiceTest (still used).
- Import order in OrderService alphabetical.
## Concerns
- Literal "payments.result" remains as arbitrary topic string in InboxEntryTest, InboxRepositoryTest, InboxServiceTest, OutboxPublisherTest (outside brief scope, harmless test data; could be renamed to orders.commands for consistency).
