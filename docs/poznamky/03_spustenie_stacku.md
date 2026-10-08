# 03 – Spustenie stacku

## Čo sa naučíš

- Celý cyklus: **teardown → build-images → deploy → port-forward → send-orders**
- Ako overiť, že stack naozaj beží (6/6 podov Ready, health endpointy)
- Ako poslať prvú objednávku a nájsť ju v **Operate** (http://localhost:8088/operate, `demo` / `demo`)
- Ako sa pripojiť k PostgreSQL, keď je port 5432 obsadený (port **15432**)

---

## Teória v skratke

### 1. Päť skriptov, päť krokov

| Skript | Čo robí | Ako často |
|---|---|---|
| `teardown.ps1` | `kubectl delete -k k8s/overlays/full` – zmaže namespace `eda-demo` **vrátane dát** PostgreSQL a Camundy (PVC) | keď chceš čistý štart |
| `build-images.ps1` | `docker build` troch služieb s tagom `:dev` | po zmene kódu |
| `deploy.ps1` | `kubectl apply -k k8s/base` a čaká na rollout všetkých workloadov | po builde / zmene manifestov |
| `port-forward.ps1` | spustí `kubectl port-forward` pre nasadené služby, Ctrl+C ukončí všetky | vždy, keď chceš na služby z Windows |
| `send-orders.ps1` | pošle N objednávok s náhodnou (alebo zadanou) sumou | na hranie |

Poradie pri deployi je dôležité: skript čaká najprv na **infraštruktúru** (`postgres`, `kafka`, `camunda`) a až potom na služby. Služby majú navyše initContainery, ktoré čakajú na svoje závislosti (`nc -z …`).

### 2. Prečo port-forward

Služby v Kubernetes majú typ `ClusterIP` – sú dostupné **len vnútri clustra**. `kubectl port-forward svc/order-service 8080:8080` otvorí tunel: `localhost:8080` na Windows → služba v clustri. Tunel žije, kým beží proces `kubectl`. Keď sa pod reštartuje, tunel na neho **spadne** a treba ho spustiť znova.

| Služba | URL na Windows |
|---|---|
| order-service | http://localhost:8080/orders |
| payment-service | http://localhost:8081/actuator/health |
| order-process | http://localhost:8082/actuator/health |
| Camunda Operate + REST API | http://localhost:8088/operate (`demo` / `demo`), http://localhost:8088/v2/... |
| PostgreSQL | `localhost:5432` (alebo `15432`, pozri nižšie) |

---

## Krok po kroku

> **Overenie:** teardown, build a deploy som pri písaní **nespúšťal znova** (zmazali by bežiace dáta). Výstupy nižšie sú skutočné výstupy z behu 2026-10-08 zachytené v `.superpowers/sdd/2026-10-08-camunda-orchestration/deploy.log` (bash varianty skriptov, PowerShell varianty robia to isté). Port-forward, objednávky, health a Operate som spustil a overil naživo.

### 1. Teardown (čistý štart)

```powershell
.\scripts\teardown.ps1
```

```
>> Namespace eda-demo removed (incl. PostgreSQL data). Local Docker images are kept.
```

Pozor: zmaže aj PVC Camundy, teda všetky inštancie procesov. Lokálne Docker image zostanú.

### 2. Build image

```powershell
.\scripts\build-images.ps1
```

Výstup (skrátený, pre každú z troch služieb to isté):

```
>> Building order-service:dev
#13 [build 5/5] RUN --mount=type=cache,target=/root/.m2 mvn -B -q package -DskipTests
#13 DONE 10.4s
#16 [extract 4/4] RUN java -Djarmode=tools -jar app.jar extract --layers --launcher --destination layers
#23 naming to docker.io/library/order-service:dev done
>> Building payment-service:dev
...
>> Building order-process:dev
#13 DONE 17.8s
#23 naming to docker.io/library/order-process:dev done
>> Done. Context docker-desktop uses local Docker images directly.
```

- `--mount=type=cache,target=/root/.m2` – Maven repozitár sa medzi buildmi cachuje, druhý build je rýchly.
- Posledný riadok: pri kontexte `docker-desktop` sa nič do minikube nenahráva.

### 3. Deploy

```powershell
.\scripts\deploy.ps1
```

```
>> Deploying profile 'base' (...\k8s\base)
namespace/eda-demo created
configmap/camunda-config created
configmap/order-process-config created
...
statefulset.apps/camunda created
statefulset.apps/postgres created
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
deployment "kafka" successfully rolled out
...
>> Waiting for deployment.apps/order-process
Waiting for deployment "order-process" rollout to finish: 0 of 1 updated replicas are available...
deployment "order-process" successfully rolled out
NAME                              READY   STATUS    RESTARTS   AGE
camunda-0                         1/1     Running   0          62s
kafka-5ffd4dbcb9-4brtd            1/1     Running   0          62s
order-process-559d94c5f7-w7zvf    1/1     Running   0          62s
order-service-775b4b98dc-g5mj9    1/1     Running   0          62s
payment-service-d8d9878d9-tg49f   1/1     Running   0          62s
postgres-0                        1/1     Running   0          62s
>> Deployed. Run scripts\port-forward.ps1 to access the services.
```

Celý deploy trval ~1 minútu, 6/6 podov Ready, 0 reštartov. Overiť si to môžeš kedykoľvek:

```powershell
kubectl -n eda-demo get deploy,statefulset
```

```
NAME                              READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/kafka             1/1     1            1           26m
deployment.apps/order-process     1/1     1            1           26m
deployment.apps/order-service     1/1     1            1           26m
deployment.apps/payment-service   1/1     1            1           26m

NAME                        READY   AGE
statefulset.apps/camunda    1/1     26m
statefulset.apps/postgres   1/1     26m
```

Camunda a PostgreSQL sú **StatefulSet** (majú PVC, dáta prežijú reštart podu). Kafka je Deployment s `emptyDir` – reštart podu Kafky = strata všetkých správ (obmedzenie dema).

### 4. Port-forward

V **samostatnom** okne PowerShellu:

```powershell
.\scripts\port-forward.ps1
```

```
order-service    http://localhost:8080/orders
payment-service  http://localhost:8081/actuator/health
order-process    http://localhost:8082/actuator/health
camunda          http://localhost:8088/operate (demo/demo)
postgres         jdbc:postgresql://localhost:5432/eda
Unable to listen on port 5432: Listeners failed to create with the following errors: [... bind: An attempt was made to access a socket in a way forbidden by its access permissions. ...]
error: unable to listen on any of the requested ports: [{5432 5432}]
Press Ctrl+C to stop.
```

Chyba pri 5432 je na tomto stroji očakávaná – port drží lokálne nainštalovaný PostgreSQL:

```powershell
Get-NetTCPConnection -LocalPort 5432 -State Listen | Select-Object LocalAddress, LocalPort, OwningProcess
Get-Process -Id 8852 | Select-Object ProcessName
```

```
LocalAddress LocalPort OwningProcess
------------ --------- -------------
::                5432          8852
0.0.0.0           5432          8852

ProcessName
-----------
postgres
```

Riešenie: forward na iný lokálny port (v ďalšom okne):

```powershell
kubectl -n eda-demo port-forward svc/postgres 15432:5432
```

```
Forwarding from 127.0.0.1:15432 -> 5432
Forwarding from [::1]:15432 -> 5432
```

```powershell
Test-NetConnection localhost -Port 15432 -InformationLevel Quiet
```

```
True
```

V DBeaveri potom: `jdbc:postgresql://localhost:15432/eda`, používateľ `order_service`, heslo podľa README (`order-service-demo`). Heslá sú demo hodnoty v manifeste [`postgres.yaml`](../../k8s/base/postgres/postgres.yaml). Pripojenie z DBeaveru som pri písaní neskúšal (na stroji nie je), port som overil cez `Test-NetConnection` a SQL dotazy v kapitolách bežia cez `psql` priamo v pode.

### 5. Health check

```powershell
curl.exe -s http://localhost:8080/actuator/health
curl.exe -s http://localhost:8081/actuator/health
curl.exe -s http://localhost:8082/actuator/health
```

```
{"groups":["liveness","readiness"],"status":"UP"}
{"groups":["liveness","readiness"],"status":"UP"}
{"groups":["liveness","readiness"],"status":"UP"}
```

### 6. Prvé objednávky

```powershell
.\scripts\send-orders.ps1 -Count 3
```

```
order 79069942-8858-4f74-9c39-4a863e3f48b0  amount 299.29  correlationId demo-1791440484-d1f3-1
order 9e9ebb71-95f7-4a92-96d2-6455b674e39c  amount 256.3  correlationId demo-1791440484-1735-2
order 8df0147e-5034-492c-8d4d-2cf5432c4ce1  amount 107.53  correlationId demo-1791440484-bc48-3
Check status: Invoke-RestMethod http://localhost:8080/orders/<id>
```

Každá objednávka dostane vlastné `correlationId` (`demo-<unix čas>-<náhoda>-<poradie>`), podľa ktorého ju neskôr nájdeš v logoch.

```powershell
Invoke-RestMethod http://localhost:8080/orders/79069942-8858-4f74-9c39-4a863e3f48b0
```

```
id            : 79069942-8858-4f74-9c39-4a863e3f48b0
customerId    : customer-1
amount        : 299.29
currency      : CZK
status        : PAID
paymentId     : 1aa82f01-50aa-4219-8c30-a18b43c4cb55
failureReason :
createdAt     : 2026-10-08T06:21:24.568270Z
updatedAt     : 2026-10-08T06:21:25.546581Z
```

`createdAt` → `updatedAt` je ~1 s: tak dlho trvala cesta cez Kafku, Camundu a payment-service.

Pri prvom overení po deployi (zachytené v `orders.txt` / `statuses.txt`) dopadlo 10 objednávok 9× `PAID` a 1× `PAYMENT_FAILED` – sedí to s mierou zamietnutia 0.2 (náhoda, môže vyjsť aj inak).

### 7. Operate

Otvor v prehliadači http://localhost:8088/operate a prihlás sa `demo` / `demo`.

```powershell
(Invoke-WebRequest -UseBasicParsing http://localhost:8088/operate).StatusCode
```

```
200
```

Čo uvidíš (prihlásenie v prehliadači potvrdil autor repa; ja som overil len, že stránka `Operate` odpovedá `200`, UI som neklikal):

- **Processes → order-fulfillment** – diagram s počtami tokenov na elementoch,
- detail inštancie – kde je token, premenné (`orderId`, `amount`, `paymentStatus`…), história krokov,
- **Incidents** – zaseknuté joby.

To isté (a overiteľne z príkazového riadku) vieš cez REST API Camundy:

```powershell
$r = Invoke-RestMethod -Method Post -Uri http://localhost:8088/v2/process-instances/search -ContentType 'application/json' -Body '{"filter":{"processDefinitionId":"order-fulfillment"},"page":{"limit":100}}'
$r.items | Group-Object state | Select-Object Name, Count
```

```
Name      Count
----      -----
COMPLETED    19
ACTIVE        1
```

Tá jedna `ACTIVE` je objednávka so sumou 666 z prvého overenia – zámerne „zaseknutá“ ([kapitola 09](09_chybove_scenare.md)).

> Prihlásenie do Operate cez `curl` (POST na `/login`) vráti `401` – UI používa vlastný login flow s CSRF. REST API `/v2/...` je v deme **bez autentizácie** (`unprotectedApi: true` v ConfigMape Camundy), preto ide bez hesla.

---

## Kód z repa

| Súbor | Čo v ňom je |
|---|---|
| [`scripts/deploy.ps1`](../../scripts/deploy.ps1) | `kubectl apply -k`, potom `rollout status` najprv pre `statefulset/postgres`, `deployment/kafka`, `statefulset/camunda`, potom pre ostatné |
| [`scripts/port-forward.ps1`](../../scripts/port-forward.ps1) | pre každú službu najprv `kubectl get svc/<x>`; ak neexistuje (napr. Grafana bez profilu monitoring), preskočí ju |
| [`scripts/send-orders.ps1`](../../scripts/send-orders.ps1) | `-Count`, `-Amount` (0 = náhodná suma 1–500), hlavička `X-Correlation-Id` |
| [`k8s/base/camunda/camunda.yaml`](../../k8s/base/camunda/camunda.yaml) | používateľ `demo/demo`, `unprotectedApi: true`, sekundárne úložisko `rdbms` → `jdbc:postgresql://postgres:5432/camunda`, PVC 1 Gi |
| [`k8s/base/postgres/postgres.yaml`](../../k8s/base/postgres/postgres.yaml) | init skript: databázy `eda` a `camunda`, schémy a používatelia (beží len nad prázdnym PVC) |

Skripty v PS 5.1 nepoužívajú `$ErrorActionPreference = 'Stop'` (okrem `send-orders.ps1`), lebo PS 5.1 by bral stderr natívnych príkazov (`docker build` píše priebeh na stderr) ako chybu. Namiesto toho kontrolujú `$LASTEXITCODE`.

---

## Bez Camundy by to vyzeralo takto

V `ba78d05` deploy čakal len na `postgres` a `kafka` a bežalo **5** podov (bez `camunda-0` a `order-process`). Port 8082 a 8088 neexistovali. Stav objednávky si videl len cez `GET /orders/{id}` a v DB – „kde visí“ objednávka si musel odvodiť z logov.

---

## Časté chyby

| Príznak | Príčina | Riešenie |
|---|---|---|
| `Unable to listen on port 5432` | Port drží lokálny PostgreSQL | `kubectl -n eda-demo port-forward svc/postgres 15432:5432` |
| `curl.exe localhost:8082` zrazu nefunguje | Pod `order-process` sa reštartoval (nový image, scale) a port-forward na neho spadol | Ukonči a spusti `port-forward.ps1` znova |
| `order-process` dlho v `Init:0/1` | initContainer čaká na Kafku a Zeebe (`nc -z camunda 26500`) | Počkaj na `camunda-0` (štart ~20–60 s); `kubectl -n eda-demo logs <pod> -c wait-for-dependencies` |
| Po deployi do starého clustra Camunda padá na DB | Databáza `camunda` neexistuje – init skript PostgreSQL beží len nad prázdnym PVC | `.\scripts\teardown.ps1` a nový deploy (README, „Upgrade existujícího prostředí“) |
| Pod v `ErrImageNeverPull` | Image `:dev` neexistuje lokálne (`imagePullPolicy: Never`) | `.\scripts\build-images.ps1` |
| Operate prihlásenie cez `curl` vráti `401` | UI má vlastný login flow | Prihlás sa v prehliadači, na skripty používaj REST API `/v2/...` |

---

## Otázky na zopakovanie

**V akom poradí deploy čaká na workloady a prečo?**
Najprv PostgreSQL, Kafka a Camunda, potom služby. Služby pri štarte potrebujú infraštruktúru (Flyway migrácie, vytvorenie topicov, nasadenie BPMN).

**Čo zmaže teardown?**
Celý namespace `eda-demo` vrátane PVC, teda dáta PostgreSQL aj Zeebe (všetky inštancie procesov). Lokálne Docker image zostanú.

**Prečo sú služby dostupné až po port-forwarde?**
Majú typ `ClusterIP`, sú viditeľné len v clustri. Port-forward je tunel z localhostu do clustra.

**Prečo REST API Camundy funguje bez hesla, ale Operate chce `demo/demo`?**
V ConfigMape je `unprotectedApi: true` (len pre demo – workery a listenery sa pripájajú bez prihlásenia). Webové UI má základnú autentizáciu s používateľom `demo`.

## Vyskúšaj si

1. Pošli 5 objednávok (`.\scripts\send-orders.ps1 -Count 5`) a potom spočítaj stavy cez REST API Camundy (príkaz z kroku 7). **Očakávanie:** `COMPLETED` stúpne o 5.
2. Otvor Operate, nájdi proces `order-fulfillment` a klikni na jednu dokončenú inštanciu. **Očakávanie:** zelená cesta `Order created → Request payment → Payment result → Payment completed? → Confirm order → Order confirmed` (alebo vetva `Cancel order`) a premenné vrátane `paymentStatus`.

---

**Ďalej:** [04 – Kafka v praxi](04_kafka_v_praxi.md)
