# Task 3 report: kostra order-process

Status: DONE. Commits created: none (per user rule).

## Implemented
New module `order-process` (package `cz.demo.eda.process`) per brief: pom.xml, Dockerfile/.dockerignore (copies), application.yml, OrderProcessApplication, config (AppConfig, EdaKafkaProperties, KafkaConfig, ProcessProperties), support (Topics, Tracing, CorrelationIdRecordInterceptor), event (DomainEvent, OrderCreated, PaymentResult/Completed/Failed, ProcessPayment, OrderCommand/ConfirmOrder/CancelOrder without `of`). Root pom: `<module>order-process</module>` added.
KafkaConfig: 6 topics as specified, MessagingMetrics removed, no setCommitRecovered.
Added a one-line Czech JavaDoc to `main()` (project rule: JavaDoc on public methods).

## Tests
EventSerializationTest (5), TopicsTest (2), CorrelationIdRecordInterceptorTest (2, copied, package changed), ProcessPropertiesTest (3). 12 pass.
RED: before implementation `mvn -q test` failed with compilation errors (cannot find symbol). GREEN: `mvn test` BUILD SUCCESS, 12/12. Root `mvn -q -DskipTests package` builds all 3 modules, exit 0.
`mvn dependency:resolve` OK: camunda-spring-boot-starter:8.10.2 and camunda-process-test-spring:8.10.2 resolve.

## Dependencies (surprising points)
- Spring Boot stays 4.1.1 (starter pulls spring-boot 4.1.1, no misalignment seen).
- Jackson 2 AND 3 both on classpath: camunda-client-java brings com.fasterxml jackson-databind 2.21.5 (+ yaml); Boot uses tools.jackson 3.1.5. Annotations (com.fasterxml.jackson.annotation 2.21) are shared, so @JsonTypeInfo works with Jackson 3 (EventSerializationTest proves it). Beware Jackson 2 databind could be picked by some autoconfig later (Task with context test).
- No Spring context test exists, so the missing BPMN has no effect yet.

## Self-review
Diff reviewed; no `var`, no stray order-service references (grep'd package/names). Imports alphabetical in new files; copied files unchanged apart from package.
