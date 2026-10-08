# 02 – Predpoklady a setup

## Čo sa naučíš

- Čo musí byť na Windows 11 nainštalované, aby demo bežalo (Docker Desktop + Kubernetes, `kubectl`)
- Čo treba navyše na build a testy mimo Dockera (JDK 21, Maven 3.9+)
- Koľko RAM stack reálne berie a prečo sa vo Windows zdá, že berie viac
- Ako sa orientovať v repe: tri samostatné Maven projekty, `k8s/`, `scripts/`

---

## Teória v skratke

### 1. Prečo Kubernetes a nie `docker compose`

Demo beží v **lokálnom Kubernetes** (Docker Desktop má jednouzlový cluster zabudovaný). Dôvod nie je pohodlie, ale realita: služby majú `readinessProbe`, `initContainers`, ConfigMapy, Secrety a PVC rovnako ako v produkcii. Napríklad `order-process` má initContainer, ktorý čaká, kým bude dostupná Kafka **aj** Zeebe – bez neho by pod padal do `CrashLoopBackOff`.

Analógia: `docker compose` je skúška v obývačke, Kubernetes je skúška na javisku. Na javisku zistíš veci, ktoré v obývačke nevidíš (napr. že pod je „Ready“ skôr, ako počúva gRPC port – uvidíme v [kapitole 09](09_chybove_scenare.md)).

### 2. Čo potrebuješ a na čo

| Nástroj | Na čo | Povinné? |
|---|---|---|
| Docker Desktop | build image (`docker build`), beh kontajnerov | áno |
| Kubernetes v Docker Desktope | beh celého stacku | áno (alebo minikube, README) |
| `kubectl` | deploy, logy, exec do podov | áno (prichádza s Docker Desktopom) |
| JDK 21 + Maven 3.9+ | `mvn test` / `mvn verify` mimo Dockera | len na testy |
| Camunda Desktop Modeler | otvoriť a upravovať BPMN | voliteľné |
| DBeaver (alebo iný SQL klient) | pozerať tabuľky inbox/outbox | voliteľné, stačí aj `psql` v pode |

Image sa buildia **v Dockeri** (multi-stage Dockerfile s Maven image), takže na samotné spustenie stacku JDK ani Maven nepotrebuješ.

### 3. Pamäť

README uvádza pre profil `base` ~2,5 GB. Reálne namerané (`docker stats`, 2026-10-08, po ~25 min behu a ~25 objednávkach):

| Kontajner | Použitá RAM / limit |
|---|---|
| camunda | 504 MiB / 1.5 GiB |
| kafka | 484 MiB / 1 GiB |
| order-service | 356 MiB / 512 MiB |
| payment-service | 352 MiB / 512 MiB |
| order-process | 265 MiB / 512 MiB |
| postgres | 82 MiB / 384 MiB |

Spolu ~2 GiB. Camunda je najväčšia položka (Zeebe + Operate + Tasklist v jednom JVM).

> **Windows/WSL2 pasca:** proces `vmmemWSL` v správcovi úloh môže ukazovať aj 12+ GB, hoci kontajnery berú ~2 GB. Je to page cache Linux VM (buildy, sťahovanie image), ktorú Windows nedostane späť sám. README popisuje jednorazové uvoľnenie aj trvalé nastavenie v `.wslconfig`. **Nepoužívaj** `autoMemoryReclaim=gradual` – Kubernetes v Docker Desktope potom nenaštartuje.

---

## Krok po kroku

Všetko spúšťaj v koreňovom adresári repa (`C:\projekty\tmp\event-driven-demo` alebo kam si ho naklonoval).

### 1. PowerShell a execution policy

```powershell
$PSVersionTable.PSVersion.ToString()
Get-ExecutionPolicy -List
```

```
5.1.26100.9444

        Scope ExecutionPolicy
        ----- ---------------
MachinePolicy       Undefined
   UserPolicy       Undefined
      Process          Bypass
  CurrentUser    RemoteSigned
 LocalMachine       Undefined
```

Skripty v `scripts\*.ps1` sa spustia, len ak policy nie je `Restricted`. Ak `CurrentUser` je `Undefined` a `LocalMachine` tiež, nastav raz:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

(Tento príkaz som pri písaní nespúšťal, policy už bola nastavená – je to štandardný príkaz PowerShellu.)

### 2. Docker Desktop a Kubernetes

V Docker Desktope: **Settings → Kubernetes → Enable Kubernetes**. Potom:

```powershell
kubectl config current-context
kubectl get nodes
docker info --format '{{.OperatingSystem}} | {{.NCPU}} CPU | {{.MemTotal}}'
```

```
docker-desktop
NAME             STATUS   ROLES           AGE    VERSION
docker-desktop   Ready    control-plane   348d   v1.32.2
Docker Desktop | 16 CPU | 10431574016
```

- Kontext musí byť `docker-desktop`. Ak nie je: `kubectl config use-context docker-desktop`.
- `MemTotal` je RAM pridelená Docker VM v bajtoch (tu ~9,7 GiB). Pre `base` stačia ~3 GiB voľné, pre profil `full` (s Elasticsearch, Kibanou a Grafanou) počítaj s ďalšími ~2 GB.
- Skripty podľa kontextu poznajú, či majú image nahrať do minikube. Pri `docker-desktop` to netreba – Kubernetes v Docker Desktope vidí lokálne image priamo.

### 3. JDK a Maven (len na testy)

```powershell
java -version
mvn -v | Select-Object -First 1
```

```
openjdk version "21.0.9" 2025-10-21
OpenJDK Runtime Environment OpenLogic-OpenJDK (build 21.0.9+10-adhoc.Administrator.jdk21u)
OpenJDK 64-Bit Server VM OpenLogic-OpenJDK (build 21.0.9+10-adhoc.Administrator.jdk21u, mixed mode, sharing)
Apache Maven 3.9.5 (57804ffe001d7215b5e7bcb531cf83df38f93546)
```

> `java -version` píše na stderr, preto PowerShell 5.1 môže nahlásiť „chybu“ (`NativeCommandError`), hoci všetko je v poriadku. Rozhoduje text výstupu.

### 4. Orientácia v repe

```powershell
Get-ChildItem -Name
Get-ChildItem order-process -Name
```

```
.idea
.superpowers
docs
k8s
order-process
order-service
payment-service
scripts
.gitattributes
.gitignore
pom.xml
README.md

src
target
.dockerignore
Dockerfile
pom.xml
```

| Priečinok | Čo obsahuje |
|---|---|
| `order-service/`, `payment-service/`, `order-process/` | **tri samostatné Maven projekty**, každý s vlastným `pom.xml` a `Dockerfile` |
| `pom.xml` (koreň) | len agregátor, aby `mvn verify` prešiel všetky tri; služby od neho nič nededia |
| `k8s/base/` | manifesty základného stacku (namespace, Kafka, PostgreSQL, Camunda, tri služby) |
| `k8s/components/`, `k8s/overlays/` | voliteľný monitoring a logging (Kustomize) |
| `scripts/` | `build-images`, `deploy`, `teardown`, `port-forward`, `send-orders` – každý ako `.sh` aj `.ps1` |
| `docs/poznamky/` | tieto poznámky |

`target/` sa objaví po prvom `mvn test`. Je v `.gitignore`.

---

## Kód z repa

| Súbor | Na čo sa pozrieť |
|---|---|
| [`pom.xml`](../../pom.xml) | agregátor `<modules>` – nič viac |
| [`order-process/pom.xml`](../../order-process/pom.xml) | Spring Boot 4.1, `camunda-spring-boot-starter`, `camunda-process-test-spring` (test) |
| [`order-process/Dockerfile`](../../order-process/Dockerfile) | multi-stage: Maven build → `jarmode=tools extract --layers` → JRE runtime, non-root používateľ |
| [`scripts/build-images.ps1`](../../scripts/build-images.ps1) | `kubectl config current-context` rozhodne, či sa volá `minikube image load` |
| [`k8s/base/kustomization.yaml`](../../k8s/base/kustomization.yaml) | zoznam zdrojov base stacku a tagy image (`:dev`) |
| [`k8s/base/order-process/order-process.yaml`](../../k8s/base/order-process/order-process.yaml) | initContainer `wait-for-dependencies` (`nc -z kafka 9092 && nc -z camunda 26500`), limity pamäte, probes |

Prečo multi-stage Dockerfile? Build (Maven + JDK) je veľký, runtime (len JRE + vrstvy JAR) malý. Vrstvy (`dependencies`, `application`…) znamenajú, že pri zmene kódu sa prebuildí len malá vrstva aplikácie a závislosti zostanú v cache.

---

## Bez Camundy by to vyzeralo takto

Setup by bol ľahší: v `ba78d05` stack tvorili len Kafka, PostgreSQL a **dve** služby. Camunda pridala:

- jeden StatefulSet s PVC (~0,5–1,5 GB RAM),
- databázu `camunda` v PostgreSQL (vytvára ju init skript, ktorý beží **len nad prázdnym PVC** – preto README pri upgrade starého prostredia vyžaduje teardown),
- tretiu službu `order-process`.

To je cena orchestrácie: ďalšia infraštruktúra, ktorú treba prevádzkovať.

---

## Časté chyby

| Príznak | Príčina | Riešenie |
|---|---|---|
| `kubectl` hlási `Unable to connect to the server` | Kubernetes v Docker Desktope nie je zapnutý alebo ešte štartuje | Settings → Kubernetes → Enable, počkaj na zelenú ikonu |
| Image sa nahrávajú do minikube, hoci ho nemáš | Kontext nie je `docker-desktop` | `kubectl config use-context docker-desktop` |
| `.\scripts\deploy.ps1 cannot be loaded because running scripts is disabled` | Execution policy `Restricted` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Kubernetes po zmene `.wslconfig` nenaštartuje (`cgroup ["kubepods"] has some missing controllers: cpuset`) | `autoMemoryReclaim=gradual` | Použi `dropcache`, `wsl --shutdown`, reštart Docker Desktopu |
| `vmmemWSL` berie 12 GB | Page cache Linux VM | `wsl -d docker-desktop sh -c "echo 3 > /proc/sys/vm/drop_caches"` (z README) |
| `mvn test` padá na Testcontainers | Nebeží Docker | Spusti Docker Desktop; testy potrebujú Docker aj mimo Kubernetes |

---

## Otázky na zopakovanie

**Potrebujem JDK a Maven, aby som spustil stack?**
Nie. Image sa buildia v Dockeri (Maven beží v build stage Dockerfile). JDK 21 a Maven treba len na testy mimo Dockera.

**Prečo sú v repe tri samostatné Maven projekty a nie multi-module s parentom?**
Aby služby nič nezdieľali (ani parent POM, ani kód). Koreňový `pom.xml` je len agregátor pre pohodlné `mvn verify`.

**Koľko RAM berie stack `base`?**
Namerané ~2 GiB, README počíta s ~2,5 GB. Najviac Camunda (~0,5 GB, limit 1,5 GiB) a Kafka (~0,5 GB).

**Prečo sa skript pýta na kubectl kontext?**
Pri `docker-desktop` vidí Kubernetes lokálne image priamo, pri minikube ich treba nahrať cez `minikube image load`.

## Vyskúšaj si

1. Spusti `docker stats --no-stream --format "{{.Name}}\t{{.MemUsage}}"` a nájdi riadky `k8s_camunda_…` a `k8s_kafka_…`. **Očakávanie:** Camunda okolo 0,5 GiB z limitu 1.5 GiB.
2. Otvor [`k8s/base/order-process/order-process.yaml`](../../k8s/base/order-process/order-process.yaml) a nájdi, na čo čaká initContainer. **Očakávanie:** na Kafku (`kafka:9092`) **a** Zeebe gRPC (`camunda:26500`) – lebo pri štarte sa vytvárajú topicy a nasadzuje BPMN.

---

**Ďalej:** [03 – Spustenie stacku](03_spustenie_stacku.md)
