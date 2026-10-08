# Task 8 report

Status: DONE (Step 6 left to the user). Commits created: none (per user rule).

## Changes
- k8s/base/postgres/postgres.yaml: Secret camunda-db, comment, init script (camunda_pw var, role camunda, DB camunda, REVOKE), env CAMUNDA_DB_PASSWORD, max_connections=60, memory 192Mi/384Mi.
- k8s/base/camunda/camunda.yaml: new, verbatim from brief.
- k8s/base/order-process/order-process.yaml: new, copy of order-service with rename; ConfigMap (KAFKA/CAMUNDA_REST/CAMUNDA_GRPC/JAVA_TOOL_OPTIONS), initContainer waits for kafka+camunda, no DB env, no DB_URL/DB_USERNAME.
- k8s/base/kustomization.yaml: comment, resources, image order-process:dev.
- scripts build-images/deploy/port-forward (.sh + .ps1) per brief (camunda rollout timeout 600s).

## Validation
- kubectl kustomize k8s/base and k8s/overlays/full: OK.
- kubectl context docker-desktop reachable; `kubectl apply -k k8s/base --dry-run=client`: all objects "created (dry run)" incl. camunda StatefulSet/Service, order-process Deployment/Service, secret/camunda-db.
- Rendered output contains camunda-db Secret, CAMUNDA_DB_PASSWORD env (postgres + camunda), camunda_pw / CREATE DATABASE camunda, order-process:dev, max_connections=60.
- bash -n on all scripts/*.sh: no errors. PowerShell parser on build-images/deploy/port-forward.ps1: 0 errors.

## Deviations / concerns
- None in content. Note: postgres init SQL only runs on empty PVC, so teardown needed (as in brief).
- git warns LF->CRLF on some files (autocrlf); files kept as they were (LF in working copy).

## Step 6 - left to the user
```
scripts/teardown.sh
scripts/build-images.sh
scripts/deploy.sh
kubectl -n eda-demo get pods
scripts/port-forward.sh      # second terminal: scripts/send-orders.sh
```
Expected: pods Ready (camunda ~2 min); orders PAID (~80 %) / PAYMENT_FAILED (~20 %); Operate http://localhost:8088/operate (demo/demo) shows order-fulfillment; amount 666 order stays active on "Payment result" and payments.commands.DLT is logged in payment-service.
