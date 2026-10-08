# Task 4 report - ProcessGateway

Status: DONE. Commits created: none (per user rule).

## Implementation
- order-process/src/main/java/cz/demo/eda/process/process/ProcessMessages.java (constants, verbatim from brief)
- order-process/src/main/java/cz/demo/eda/process/process/ProcessGateway.java (brief code + Czech JavaDoc on the constructor)
- order-process/src/test/java/cz/demo/eda/process/process/ProcessGatewayTest.java (4 tests from brief)

## Jar verification (camunda-client-java-8.10.2, javap)
- messageName -> PublishMessageCommandStep1.PublishMessageCommandStep2; correlationKey -> ...Step3 (as in brief).
- Step3 has messageId(String), timeToLive(Duration), variables(Map<String,Object>) - OK.
- ClientStatusException(Status, Throwable), getStatusCode() -> Status.Code - OK (final class, extends ClientException).
- CamundaFuture.join() exists; CamundaClientFutureImpl references ClientStatusException (gRPC errors mapped to it). Real runtime behaviour is not covered by a unit test (mock only).

## TDD
- RED: compile error "cannot find symbol: class ProcessGateway".
- GREEN: 4/4 after implementation, with one test fix (below). Full `mvn test` in order-process: 16 tests, 0 failures, BUILD SUCCESS.

## Deviations
1. Test setUp: added `clearInvocations(command)` (+ static import) after the stubbing. Reason: the `when(command.messageId(..)...)` stubbing itself invokes messageId() on the mock, so `verify(command).messageId(..)` failed with TooManyActualInvocations (2 instead of 1). Behaviour of production code unchanged.
2. Added Czech JavaDoc to the ProcessGateway constructor (project rule: JavaDoc on every public constructor).
No type/import name differences from the brief.

## Self-review
- No var, no empty catch, SLF4J, imports ordered as in the brief (project style: third-party, then java.*).
- Only ClientStatusException is caught; with gRPC preference ALREADY_EXISTS maps to false, other codes rethrown.

## Concerns
- Brief's "alphabetical imports": kept brief's order (java.* group after third-party), consistent with existing module files.
