### Task 8: Kubernetes – Camunda, databáze a nasazení order-process

**Files:**
- Create: `k8s/base/camunda/camunda.yaml`
- Create: `k8s/base/order-process/order-process.yaml`
- Modify: `k8s/base/postgres/postgres.yaml`, `k8s/base/kustomization.yaml`
- Modify: `scripts/build-images.sh`, `scripts/build-images.ps1`, `scripts/deploy.sh`, `scripts/deploy.ps1`, `scripts/port-forward.sh`, `scripts/port-forward.ps1`

**Interfaces:**
- Produces: Service `camunda` (porty `http` 8080, `grpc` 26500, `management` 9600); Service `order-process` (8080); Secret `camunda-db`; DB `camunda`, role `camunda`.

- [ ] **Step 1: PostgreSQL – databáze pro Camundu**

`postgres.yaml`:
- přidat Secret:

```yaml
---
apiVersion: v1
kind: Secret
metadata:
  name: camunda-db
type: Opaque
stringData:
  password: camunda-demo
```

- init skript `01-service-schemas.sh` – doplnit proměnnou a SQL (CREATE DATABASE nesmí běžet v transakci, psql heredoc běží v autocommitu):

```sh
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
         -v order_pw="$ORDER_DB_PASSWORD" -v payment_pw="$PAYMENT_DB_PASSWORD" \
         -v camunda_pw="$CAMUNDA_DB_PASSWORD" <<'EOSQL'
    CREATE ROLE order_service LOGIN PASSWORD :'order_pw';
    CREATE ROLE payment_service LOGIN PASSWORD :'payment_pw';
    -- Schéma vlastní služba; cizí role na něj nemá USAGE, takže do něj nevidí.
    CREATE SCHEMA orders AUTHORIZATION order_service;
    CREATE SCHEMA payments AUTHORIZATION payment_service;
    REVOKE ALL ON DATABASE eda FROM PUBLIC;
    GRANT CONNECT ON DATABASE eda TO order_service, payment_service;
    -- Camunda (vedlejší úložiště pro Operate) má vlastní databázi; schéma si zakládá sama.
    CREATE ROLE camunda LOGIN PASSWORD :'camunda_pw';
    CREATE DATABASE camunda OWNER camunda;
    REVOKE ALL ON DATABASE camunda FROM PUBLIC;
    EOSQL
```

- do env kontejneru `postgres` přidat:

```yaml
            - name: CAMUNDA_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: camunda-db
                  key: password
```

- `args`: `max_connections=40` → `max_connections=60` (Camunda si drží vlastní pool); limit paměti `256Mi` → `384Mi` (request `128Mi` → `192Mi`).
- Komentář na začátku souboru doplnit: „Camunda má vlastní databázi camunda. Init skript běží jen nad prázdným PVC – po upgradu existujícího prostředí je nutný teardown.“

- [ ] **Step 2: `k8s/base/camunda/camunda.yaml`**

```yaml
# Camunda 8 Orchestration Cluster (Zeebe + Operate + Tasklist v jednom kontejneru), 1 broker.
# Primární úložiště (log událostí + RocksDB) je na PVC – to je zdroj pravdy o běžících instancích.
# Vedlejší úložiště pro Operate/Tasklist/search API je databáze "camunda" v PostgreSQL (plní ji exportér).
apiVersion: v1
kind: ConfigMap
metadata:
  name: camunda-config
  labels:
    app.kubernetes.io/name: camunda
data:
  application.yaml: |
    camunda:
      system:
        cpu-thread-count: "2"
        io-thread-count: "2"
      security:
        authentication:
          method: basic
          # Workery a listenery v namespace se připojují bez přihlášení – jen pro demo.
          unprotectedApi: true
        authorizations:
          enabled: false
        initialization:
          users:
            - username: demo
              password: demo
              name: Demo User
              email: demo@demo.com
          defaultRoles.admin.users:
            - demo
      data:
        secondary-storage:
          type: rdbms
          rdbms:
            url: jdbc:postgresql://postgres:5432/camunda
            username: camunda
            password: ${CAMUNDA_DB_PASSWORD}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: camunda
  labels:
    app.kubernetes.io/name: camunda
spec:
  serviceName: camunda
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: camunda
  template:
    metadata:
      labels:
        app.kubernetes.io/name: camunda
        app.kubernetes.io/part-of: eda-demo
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/path: /actuator/prometheus
        prometheus.io/port: "9600"
    spec:
      securityContext:
        # Image běží jako uživatel 1001 – PVC musí být zapisovatelné pro jeho skupinu.
        fsGroup: 1001
      initContainers:
        - name: wait-for-postgres
          image: busybox:1.37.0
          command: ["sh", "-c", "until nc -z postgres 5432; do echo waiting for postgres; sleep 2; done"]
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 50m
              memory: 32Mi
      containers:
        - name: camunda
          image: camunda/camunda:8.10.2
          ports:
            - name: http
              containerPort: 8080
            - name: grpc
              containerPort: 26500
            - name: management
              containerPort: 9600
          env:
            - name: CAMUNDA_DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: camunda-db
                  key: password
            - name: JAVA_TOOL_OPTIONS
              value: -XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError
          volumeMounts:
            - name: data
              mountPath: /usr/local/camunda/data
            - name: config
              mountPath: /usr/local/camunda/config/application.yaml
              subPath: application.yaml
          resources:
            requests:
              cpu: 250m
              memory: 1Gi
            limits:
              cpu: "2"
              memory: 1536Mi
          startupProbe:
            httpGet:
              path: /actuator/health/liveness
              port: management
            timeoutSeconds: 3
            periodSeconds: 5
            failureThreshold: 60
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: management
            timeoutSeconds: 3
            periodSeconds: 15
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: management
            timeoutSeconds: 3
            periodSeconds: 5
            failureThreshold: 3
      volumes:
        - name: config
          configMap:
            name: camunda-config
  # Běžící instance procesů přežijí restart podu; smaže je až teardown (smazání namespace).
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: camunda
  labels:
    app.kubernetes.io/name: camunda
spec:
  selector:
    app.kubernetes.io/name: camunda
  ports:
    - name: http
      port: 8080
      targetPort: http
    - name: grpc
      port: 26500
      targetPort: grpc
    - name: management
      port: 9600
      targetPort: management
```

- [ ] **Step 3: `k8s/base/order-process/order-process.yaml`**

Kopie `k8s/base/order-service/order-service.yaml` s náhradou `order-service` → `order-process` všude (ConfigMap, Deployment, labels, image `order-process:dev`, Service) a s těmito rozdíly:
- ConfigMap `data`:

```yaml
  KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  CAMUNDA_REST_ADDRESS: http://camunda:8080
  CAMUNDA_GRPC_ADDRESS: http://camunda:26500
  # Heap jen 60 % limitu – zbytek potřebuje metaspace, vlákna, gRPC a direct buffery Kafka klienta.
  JAVA_TOOL_OPTIONS: -XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError
```

- initContainer: komentář „Čeká na Kafku a Zeebe: KafkaAdmin při startu vytváří topicy a @Deployment nasazuje BPMN.“, příkaz `until nc -z kafka 9092 && nc -z camunda 26500; do echo waiting for kafka and camunda; sleep 2; done`
- bez `env` s `DB_PASSWORD` (služba nemá DB).

- [ ] **Step 4: kustomization**

`k8s/base/kustomization.yaml`: komentář „Základní deploy: Kafka, PostgreSQL, Camunda a tři služby…“; `resources` doplnit `- camunda/camunda.yaml` (za postgres) a `- order-process/order-process.yaml`; `images` doplnit:

```yaml
  - name: order-process
    newTag: dev
```

Run: `kubectl kustomize k8s/base > /dev/null && kubectl kustomize k8s/overlays/full > /dev/null`
Expected: bez chyby.

- [ ] **Step 5: Skripty**

`scripts/build-images.sh`: `for service in order-service payment-service order-process; do`, grep `'(order|payment)-(service|process)'`, komentář „Sestaví Docker image všech služeb…“.
`scripts/build-images.ps1`: `foreach ($service in 'order-service', 'payment-service', 'order-process')`, `Select-String -Pattern '(order|payment)-(service|process)'`, stejný komentář.

`scripts/deploy.sh`: infrastruktura `for workload in statefulset/postgres deployment/kafka statefulset/camunda; do` s `--timeout=600s` (Camunda startuje pomaleji); komentář použití „(výchozí base = Kafka, PostgreSQL, Camunda, služby)“.
`scripts/deploy.ps1`: `foreach ($workload in 'statefulset/postgres', 'deployment/kafka', 'statefulset/camunda')` s `--timeout=600s`; stejný komentář.

`scripts/port-forward.sh` – do `FORWARDS` za `payment-service` přidat:

```bash
  "order-process   8082:8080  http://localhost:8082/actuator/health"
  "camunda         8088:8080  http://localhost:8088/operate (demo/demo)"
```

`scripts/port-forward.ps1` – obdobně:

```powershell
    @('order-process', '8082:8080', 'http://localhost:8082/actuator/health'),
    @('camunda', '8088:8080', 'http://localhost:8088/operate (demo/demo)'),
```

(`8088` odpovídá výchozí `CAMUNDA_REST_ADDRESS` v `order-process/application.yml` – lokální spuštění služby proti clusteru přes port-forward funguje bez konfigurace; gRPC 26500 se pro lokální běh forwarduje ručně.)

- [ ] **Step 6: Ověření v clusteru**

Run:
```bash
scripts/teardown.sh          # init skript PostgreSQL musí proběhnout znovu (nová DB camunda)
scripts/build-images.sh
scripts/deploy.sh
kubectl -n eda-demo get pods
```
Expected: všechny pody `Running`/`Ready` (camunda do ~2 min).

Run: `scripts/port-forward.sh` a v druhém terminálu `scripts/send-orders.sh`
Expected:
- `curl localhost:8080/orders/<id>` → objednávky v `PAID` (~80 %) a `PAYMENT_FAILED` (~20 %)
- `http://localhost:8088/operate` (demo/demo) → proces `order-fulfillment`, dokončené instance v obou větvích
- objednávka s částkou 666 (`curl -X POST localhost:8080/orders -H 'Content-Type: application/json' -d '{"customerId":"c","amount":666,"currency":"CZK"}'`) zůstane v Operate aktivní na `Payment result`, v logu payment-service je záznam z `payments.commands.DLT`.

---

