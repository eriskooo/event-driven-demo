# Tutorial report – docs/poznamky (2026-10-08)

Status: DONE_WITH_CONCERNS (all chapters written; a few items not verifiable live; README/code discrepancies listed below, not fixed).

## Files written
- docs/poznamky/00_osnova.md … 13_zhrnutie.md (14 files, ~3 760 lines, Slovak, structure per brief)
- README.md: added section "Poznámky na samoštúdium" linking docs/poznamky/00_osnova.md (only change outside docs/poznamky)
- All relative links in docs/poznamky checked – none missing.

## Verified live (PowerShell 5.1 unless noted; outputs pasted shortened)
- Env: PS version, execution policy, kubectl context/nodes, docker info, java/mvn versions, docker stats per container (ch02)
- Port-forward 15432 (host Postgres holds 5432), health endpoints, send-orders.ps1, manual POST via curl.exe, GET order, Operate page HTTP 200 (ch03)
- Camunda REST: topology, process-definitions, process-instances, variables (incl. scopeKey duplication), element-instances, message-subscriptions, correlated-message-subscriptions, incidents; search-API visibility delay ~695 ms (ch06/07)
- Kafka CLI in pod: topics list/describe, consumer groups (incl. --all-groups), console-consumer with headers/partition, same-partition-per-key across 4 topics (ch04)
- psql in pod: orders/inbox/outbox counts, single-order rows, poison inbox row, \d, schema isolation (permission denied), camunda DB tables; SKIP LOCKED two-session demo + variant without SKIP LOCKED (waited 4.2 s) (ch05, ch12)
- Duplicates: duplicate ProcessPayment → inbox "Duplicate … skipped", still 1 payment; duplicate PaymentResult → Zeebe ALREADY_EXISTS "Duplicate message … skipped"; new-eventId PaymentResult for finished order → published, no effect (ch05/08)
- commandId = UUID v3 from jobKey recomputed in PowerShell, matches logs (ch08)
- Poison 666: retry 500/1000/2000 ms → DLT, DLT content with eda-dlt-* headers, order PENDING_PAYMENT, instance ACTIVE; manual rescue via PaymentFailed → PAYMENT_FAILED, path via cancel_order (ch09)
- Temporary scale order-process 1→0→1 (lag 1, then PAID) and camunda 1→0→1 (OrderCreated → orders.created.DLT after ~3.5 s; redrive with original eventId → PAID). Both restored to 1 replica and Ready; port-forwards 8082/8088 restarted (running as background kubectl processes).
- Logs by correlationId across 3 services (tutorial-1, cvicenie-1), MDC fields jobKey/processInstanceKey, eda_* and camunda_client_worker_* metrics, zeebe_* metrics via temporary port-forward 19600 (ch06/08/11)
- `kubectl kustomize k8s/overlays/full` render only (ch11)
- Tests: mvn test order-process 42, order-service 96, payment-service 94 – 0 failures (full runs via Git Bash with identical mvn args; a 2-class run verified in PowerShell)

## Not verified (stated explicitly in the chapters)
- Teardown / build-images / deploy: documented from captured deploy.log, orders.txt, statuses.txt, pf.log, progress.md (forbidden to rerun)
- Operate UI clicking/login (only HTTP 200 of /operate; login confirmed by user per progress.md)
- DBeaver connection (not installed; port checked via Test-NetConnection)
- Camunda Desktop Modeler (not installed)
- Incident creation (would require Kafka outage; Kafka uses emptyDir → data loss); Kafka outage scenario overall
- Message TTL expiry (1 h); "PaymentResult arrives before process waits" only via OrderProcessIntegrationTest
- `Set-ExecutionPolicy` (policy already set), `mvn verify` from root (ran the three projects separately)
- Monitoring/logging/full overlays (not deployed)
- Exercises 9 (timer) and 10 (new BPMN step) – code/BPMN changes, solution sketches only

## Side effects on the running demo (for awareness)
- Extra orders created (tutorial-1, lag-test-1, scale-test-1, zeebe-down-1, cvicenie-1, one without header, poison 1c116cc8)
- Poison order 1c116cc8 resolved manually to PAYMENT_FAILED; original poison debb6888 left ACTIVE (as README describes)
- Manually produced messages: duplicate ProcessPayment, duplicate PaymentResult, one PaymentResult with new eventId for finished order 3101528e (buffered in Zeebe for 1 h, harmless), PaymentFailed for 1c116cc8, two redrives of OrderCreated for 707fa902 (first went to orders.created.DLT again)
- order-process pod and camunda-0 pod recreated by scale; mvn created target/ dirs (gitignored). No git writes, no apply/delete, no code/manifest changes.

## Facts in README/code that look wrong or imprecise (not fixed)
1. README "Proč inbox + outbox": dedup via `INSERT … ON CONFLICT DO NOTHING` – code `InboxService.store` uses `existsById` + `save` (race → PK violation → Kafka redelivery). Same outcome, different mechanism.
2. README "Zeebe nikoho nevolá … (pull / job streaming)": job streaming is not active – broker metric `zeebe_broker_jobs_pushed_count_total` = 0 while jobs were activated (JOB_BATCH exported); workers use long-polling ActivateJobs.
3. README demo step 5 (Kibana `correlationId` = "celá cesta objednávky přes všechny tři služby"): fluent-bit.yaml tails only order-service-* and payment-service-* logs; order-process logs are not shipped.
4. README "Známé chování dema": "objednávka zůstane `CREATED`/`PENDING_PAYMENT`" – OrderStatus has no CREATED state (only PENDING_PAYMENT, PAID, PAYMENT_FAILED).
5. README "Známé chování dema": "hodil by se timer boundary event" on Payment result – `payment_result` is an intermediate catch event; BPMN boundary events attach only to activities. Needs receive task + boundary timer or event-based gateway (explained in ch12, exercise 9).
6. Operational observation: after `camunda-0` becomes Ready (readiness on port 9600), gRPC 26500 kept refusing connections for ~40 s; an OrderCreated redrive in that window went to DLT again. Readiness probe does not cover the gateway port.
7. Minor: orders.txt shows older send-orders hint "Check status: curl …"; current send-orders.ps1 prints "Invoke-RestMethod …" (just a newer script version, fine).
