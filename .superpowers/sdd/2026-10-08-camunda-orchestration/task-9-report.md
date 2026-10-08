# Task 9 report: README

Status: DONE. Commits created: none (per user rule). Maven build not run (controller verifies).

## Sections changed in README.md
Intro; Architektura (new mermaid from brief); Tok objednávky (5 steps, orchestration); new "Proces v Camundě"
(ASCII flow, BPMN element table, "Zeebe nikoho nevolá", Idempotence table, Úložiště stavu, Operate, Modelování,
Známé chování dema) placed under Architektura as ### with #### subsections; Databáze (camunda row, camunda-db Secret,
teardown note); Kontrakt zpráv (topic table + JSON for payments.commands, orders.commands, DLT per service);
Co projekt demonstruje (4 new rows, consumer groups, ack row incl. order-process); Verze (Camunda row);
Předpoklady/Paměť (+~1.5 GiB, minikube 10 GB); Deploy table; Port-forwardy (8082, 8088 demo/demo); posílání objednávek;
Demo DLT (payments.commands.DLT); Konfigurace (all services, message-ttl, CAMUNDA_REST_ADDRESS); Struktura;
Co bylo ověřeno (old results labelled as choreography version; e2e test noted; k8s deploy not run); Omezení.

## Facts verified against
order-fulfillment.bpmn (element ids), scripts/port-forward.sh (ports), scripts/deploy.sh and build-images.sh,
k8s/base/camunda/camunda.yaml (image 8.10.2, PVC, memory 1-1.5Gi), k8s/base/postgres/postgres.yaml (camunda DB/role/Secret camunda-db),
k8s/base/order-process/order-process.yaml (ConfigMap order-process-config), order-process application.yml
(group order-process, ack-mode record, message-ttl 1m, prefer-rest-over-grpc false), CommandPublisher.commandId, spec.

## Grep (orders.created.DLT|PaymentResultListener|OrderCreatedListener)
20 mermaid (order-process), 55 and 66 flow steps (order-process), 208 `orders.created.DLT` listed as order-process DLT. No choreography remnants.
Mermaid: 4 subgraph / 4 end.

## Concerns
- Minikube memory recommendation raised 8192 -> 10240 and base RAM estimate ~2.5 GB are my estimates from the manifests' limits.
- Orchestrated k8s deployment unverified (stated in README).
- Mermaid diagram follows the brief (simplified domain-service detail, no per-listener inbox/outbox internals).
