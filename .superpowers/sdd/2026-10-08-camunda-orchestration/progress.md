# SDD ledger — plan: docs/superpowers/plans/2026-10-08-camunda-orchestration.md
Spec: docs/superpowers/specs/2026-10-08-camunda-orchestration-design.md

Ruling: No branch/worktree/commits; work in main working tree, per-task diffs via snapshots (snap.sh/pkg.sh) — user's global rule "Necommituj, git operace budu dělat sám" overrides skill's commit flow — cost if wrong: none to code; user commits manually.
Ruling: Docker not running at start; implementers run container tests if Docker available, else unit tests only and report it — environment, user asked to start Docker — cost: integration regressions caught later (final verification reruns all).

## Pre-flight scan
| Pair / task | Produces vs consumes | Finding |
|---|---|---|
| T1 ↔ T6 | T1 payment-service consumes payments.commands JSON {eventId,timestamp,correlationId,orderId,amount,currency}; T6 RequestPaymentWorker produces ProcessPayment same fields | consistent |
| T2 ↔ T6 | T2 consumes orders.commands {type ConfirmOrder: paymentId / CancelOrder: reason}; T6 produces same via OrderCommand JsonTypeInfo copy (T3) | consistent |
| T3 ↔ T4/T5/T6 | T3 ProcessProperties.messageTtl, Topics, events, Tracing; consumed with same names | consistent |
| T4 ↔ T5 | ProcessGateway.publish(String,String,UUID,Map):boolean; listeners call with same signature | consistent |
| T5 ↔ T6 | variables orderId/amount/currency/correlationId/paymentStatus/paymentId/failureReason ↔ OrderVariables fields | consistent |
| T4/T6 ↔ T7 | job types request-payment/confirm-order/cancel-order, messages OrderCreated/PaymentResult, PROCESS_ID order-fulfillment ↔ BPMN | consistent |
| T7 ↔ T8 | camunda addresses default localhost:8088/26500 ↔ port-forward 8088 | consistent |
| T3 KafkaConfig | copy of order-service minus MessagingMetrics | order-process has no MessagingMetrics class — plan says remove; consistent |
| T1 self | integration test sendRaw to orders.created relies on broker auto-create | Testcontainers apache/kafka default auto-create on — ok |
| T2 self | OrderServiceTest renames; OrderCommandListenerTest copy | consistent |
| T4 self | ProcessGatewayTest deep stubs; LENIENT added | ok |
| T7 self | CPT purge assumption flagged with fallback in plan | ok |
| Global | plan steps "commit" absent; Global Constraints say no commit | consistent with user rule |
Task 1: dispatched implementer (sonnet), base snapshot t0
Task 1: minor (deferred): ProcessPaymentListenerTest DisplayName "Objednávku uloží…" + helper order() stale terminology
Task 1: minor (deferred): non-alphabetical imports (ProcessPayment before PaymentCompleted) in Payment/PaymentService/PaymentSimulator/PaymentServiceTest
Task 1: minor (deferred): negative integration test uses 3 s absence window (plan-mandated, inherent)
Task 1: complete (snapshots t0..t1, review clean, 94 tests incl. Testcontainers)
Task 2: dispatched implementer (sonnet), base snapshot t1
Task 2: minor (deferred): literal topic 'payments.result'/'orders.created' remains as fixture data in inbox/outbox tests of both services (outside brief)
Task 2: minor (deferred): OrderServiceTest still has "výsledkem platby" wording in 2 tests (should_keepFinalState_whenSecondResultArrives, unknown-order test)
Task 2: minor (deferred): OrderServiceTest import order OrderCreated/CancelOrder/ConfirmOrder
Task 2: minor (deferred): OrderCommandHandlerTest happy path only CancelOrder
Task 2: complete (snapshots t1..t2, review clean, 101 tests incl. Testcontainers)
Task 3: dispatched implementer (sonnet), base snapshot t2
Task 3: minor (deferred): ProcessProperties compact ctor without JavaDoc
Task 3: minor (deferred): EventSerializationTest covers only orderId null check (plan-mandated tests)
Task 3: minor (deferred): Jackson 2 (camunda client) + Jackson 3 (Boot) both on classpath — watch in context test (Task 7)
Task 3: complete (snapshots t2..t3, review clean, 12 tests)
Task 4: dispatched implementer (sonnet), base snapshot t3
Task 4: minor (deferred): ProcessGatewayTest proves args indirectly via deep stubs (no InOrder/captors); exception tests check type not identity
Task 4: minor (deferred): ProcessGateway success log at info (could be debug)
Task 4: complete (snapshots t3..t4, review clean, 16 tests)
Task 5: dispatched implementer (haiku, complete code in brief), base snapshot t4
Task 5: ⚠️ resolved by controller: sealed PaymentResult polymorphism covered by Task 3 EventSerializationTest.should_deserializeSubtype_whenReadAsPaymentResult; group-id order-process set in application.yml; per-listener default.type wiring verified by Task 7 e2e test
Task 5: minor (deferred): PaymentResultListenerTest lacks gateway-failure propagation test
Task 5: minor (deferred): PaymentResultListener has no debug log for correlated/duplicate
Task 5: complete (snapshots t4..t5, review clean, 23 tests)
Task 6: dispatched implementer (haiku, complete code in brief), base snapshot t5
Task 6: minor (deferred, plan-mandated, violates user global rule → fix in final wave): public constructors without Czech JavaDoc in CommandPublisher + 3 workers (also Task 3 ProcessProperties)
Task 6: minor (deferred): CommandPublisher interrupt/timeout paths untested; timeout leaves future running (harmless via inbox dedupe — add comment)
Task 6: minor (deferred): RED evidence summarized, not raw output; straight closing quote in worker JavaDoc
Task 6: ⚠️ resolved by controller: KafkaTemplate/Clock beans + @JobWorker registration verified by Task 7 Spring context e2e test
Task 6: complete (snapshots t5..t6, review clean, 32 tests)
Task 7: dispatched implementer (opus — CPT/Kafka e2e with unverified API assumptions), base snapshot t6
Task 7: minor (deferred, plan-mandated): unknown-order PaymentResult test is absence-only (no positive proof of consumption), 1 s window on orders.commands thin
Task 7: minor (deferred, plan-mandated): should_moveToDlt_whenOrderCreatedUnreadable DisplayName claims "proces nespustí" but not asserted
Task 7: minor (deferred): absence windows 1–2 s; byProcessId isolation relies on CPT purge; gateway with missing paymentStatus → incident not default flow
Task 7: complete (snapshots t6..t7, review clean, 7 e2e + root build 240 tests green)
Ruling: Task 8 Step 6 (teardown + redeploy to local k8s) NOT executed by subagent — teardown deletes namespace incl. PVC data (destructive, outside worktree); implementer validates manifests with kubectl kustomize/dry-run only; cluster run left to user — cost if wrong: manifest runtime issues (e.g. Camunda env/config) surface only when user deploys
Task 8: dispatched implementer (sonnet), base snapshot t7
Task 8: minor (deferred, runtime-unverified): camunda secondary-storage rdbms keys, defaultRoles.admin.users dotted key, prometheus endpoint on 9600 — confirm on real deploy
Task 8: complete (snapshots t7..t8, review clean, static validation + client dry-run OK; Step 6 left to user per ruling)
Task 9: dispatched implementer (sonnet), base snapshot t8
User approved (2026-10-08): teardown + real k8s deploy for verification; tutorial poznamky/ (self-study variant, Slovak, Liferay-notes style) after code work
Task 9: implementer done (README), base t8 → t9; concerns: own RAM figures, README states k8s deploy not yet run (update after real deploy)
Task 9: minor → fix in final wave: README topic table says *.DLT 1 partition, KafkaConfig creates DLT with same partitions (3) — factual error (also plan spec table said 1)
Task 9: minor (deferred): README token sketch ASCII not mermaid; DLT write-only note; duplicated RAM line
Task 9: complete (snapshots t8..t9, review approved)
K8s verification (2026-10-08, docker-desktop): teardown+build+deploy exit 0, 6/6 pods Ready; 10 orders → 9 PAID, 1 PAYMENT_FAILED; 666 → PENDING_PAYMENT; Camunda search: 10 COMPLETED + 1 ACTIVE at payment_result; payment-service DLT listener logged payments.commands.DLT; ERROR logs only the 2 expected poison lines; Operate reachable via 8088. Port-forward of postgres 5432 fails locally (port taken by host, pre-existing, unrelated). Camunda runtime config (rdbms keys, demo user) confirmed working.
Final review: With fixes. Important: (1) message TTL 1m < job timeout → stuck instance; (2) Camunda memory OOM risk.
Ruling: Important #1 fixed via TTL 1h (option a) — cheapest, covers job timeout + incident resolution; option b (re-publish stored result) is a design change beyond spec — cost if wrong: an incident unresolved >1h still strands the instance (documented).
Ruling: Important #2 not changed — measured on running deploy: peak 550 MB / 1536Mi, 0 restarts; static estimate not confirmed — cost if wrong: OOMKill under heavy load; README states measured peak.
Ruling: final fix wave includes reviewer's "opravit" minors + final-review minors 1–7; leaves the rest (see triage) — cost: cosmetic leftovers.
Open: Operate login via curl POST /login demo/demo → 401 though user demo exists; asked user to verify in browser.
Resolved: Operate login demo/demo works in browser (user confirmed 2026-10-08); curl 401 was a wrong-endpoint/CSRF artefact
Final fix wave: re-review all F1–F8 addressed (snapshots t9..t10); running clean mvn test for verification
Ruling: .superpowers/ kept and tracked in git per user request (instead of deleting workspace at end); snap-*/, pf.log, final-test.log ignored — cost: sdd-workspace script rewrites .gitignore to '*' if run again
Final verification: mvn clean test exit 0 — order-service 96, payment-service 94, order-process 42 (8 e2e), 0 failures. order-process image rebuilt + rollout; 5 orders → 3 PAID, 2 PAYMENT_FAILED; correlationId in order-process logs.
Docs: README BPMN mermaid diagram added (rendered OK via mermaid-cli).
Tutorial: dispatched writer (opus) → docs/poznamky/
