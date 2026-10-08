# 00 – Osnova: Event-driven a Camunda po lopate (samoštúdium)

Cieľ: pochopiť **event-driven architektúru** a **stavový automat v Camunde 8** na jednom konkrétnom, bežiacom príklade. Po prečítaní by si mal vedieť demo z tohto repa **spustiť**, **sledovať** cestu objednávky cez Kafku, PostgreSQL a Zeebe a **vysvetliť**, prečo je každý kúsok kódu napísaný práve tak.

Nie je to kurz Kafky ani Camundy od nuly. Berieme z nich len to, čo demo reálne používa. Nie je to ani príprava na pohovor: namiesto „otázok na pohovor“ má každá kapitola otázky na zopakovanie a malý experiment.

---

## Ako čítať tieto poznámky

- Kapitoly čítaj **v poradí**. Každá stavia na predchádzajúcej. Jedna kapitola je na 20 až 40 minút.
- Príkazy reálne spúšťaj. Pri každom je uvedený **očakávaný výstup** (skrátený), takže hneď vidíš, či ti to funguje. Tvoje ID objednávok, kľúče a časy budú, samozrejme, iné.
- Každý príkaz v kapitolách sa pri písaní reálne spustil na Windows 11 proti stacku v Kubernetes z Docker Desktopu (2026-10-08). Ak niečo nešlo overiť, kapitola to výslovne uvedie.
- Príkazy sú pre **Windows PowerShell 5.1**: žiadne `&&`, namiesto `curl` píšeme `curl.exe` (v PS 5.1 je `curl` alias na `Invoke-WebRequest`). Bash varianty skriptov (`*.sh`) robia to isté.
- Kód sa neprepisuje. Kapitoly vysvetľujú existujúce projekty:
  - [`order-service/`](../../order-service) – REST API objednávok, inbox, outbox
  - [`payment-service/`](../../payment-service) – simulácia platby, inbox, outbox, DLT listener
  - [`order-process/`](../../order-process) – orchestrátor: BPMN proces v Camunde, Kafka listenery, job workery
- Celkový prehľad, verzie a konfigurácia sú v [`README.md`](../../README.md) (po česky). Poznámky ho vysvetľujú krok po kroku.

### Štruktúra každej kapitoly (01–11)

1. **Čo sa naučíš** – 2 až 4 body
2. **Teória v skratke** – pár odsekov, analógie, tabuľky, kde pomôže, aj diagram
3. **Krok po kroku** – PowerShell príkazy + očakávaný výstup
4. **Kód z repa** – konkrétne súbory s vysvetlením
5. **Bez Camundy by to vyzeralo takto** – porovnanie s pôvodnou choreografiou (git commit `ba78d05`), kde to dáva zmysel
6. **Časté chyby** – príznak → príčina → riešenie
7. **Otázky na zopakovanie** + **Vyskúšaj si**

Kapitola 12 sú cvičenia s riešením, kapitola 13 zhrnutie.

---

## Mapa kapitol

| # | Kapitola | Čo z nej máš |
|---|---|---|
| 01 | [Čo je event-driven](01_co_je_event_driven.md) | Event vs. príkaz, choreografia vs. orchestrácia, prečo stavový automat, mapa repa |
| 02 | [Predpoklady a setup](02_predpoklady_a_setup.md) | Docker Desktop + Kubernetes, JDK 21, Maven, RAM, orientácia v repe |
| 03 | [Spustenie stacku](03_spustenie_stacku.md) | Build, deploy, port-forward, prvá objednávka, Operate, Postgres |
| 04 | [Kafka v praxi](04_kafka_v_praxi.md) | Topicy, partície, kľúč `orderId`, consumer groups, ack módy, Kafka CLI v pode |
| 05 | [Outbox a inbox](05_outbox_a_inbox.md) | Dual write, `OutboxRelay`, deduplikácia v inboxe, retry s backoffom, DLT, `SKIP LOCKED`, SQL |
| 06 | [Camunda a Zeebe](06_camunda_a_zeebe.md) | Čo je Zeebe, pull model, primárne vs. sekundárne úložisko, exportér, Operate, REST API |
| 07 | [BPMN proces](07_bpmn_proces.md) | `order-fulfillment.bpmn` element po elemente, token, gateway, correlation key, Modeler |
| 08 | [Most Zeebe ↔ Kafka](08_most_zeebe_kafka.md) | `ProcessGateway`, listenery, workery, `CommandPublisher`, `messageId = eventId`, `commandId` z `jobKey`, TTL |
| 09 | [Chybové scenáre](09_chybove_scenare.md) | Poison 666, incident, duplicity, výpadok Zeebe, predbehnutá správa, TTL, redrive |
| 10 | [Testovanie](10_testovanie.md) | Unit testy, Testcontainers, Camunda Process Test |
| 11 | [Observabilita](11_observabilita.md) | `correlationId` v hlavičke a MDC, ECS JSON logy, metriky, Grafana/Kibana overlay |
| 12 | [Cvičenia](12_cvicenia.md) | 10 cvičení so zadaním, nápovedou a riešením |
| 13 | [Zhrnutie](13_zhrnutie.md) | 10 hlavných bodov |

---

## Slovník pojmov

Netreba sa ho učiť naspamäť. Vráť sa sem, keď v kapitole narazíš na neznáme slovo.

| Pojem | Čo to je | Kde to v deme nájdeš |
|---|---|---|
| **Event (udalosť)** | Správa o niečom, čo sa **už stalo**. Odosielateľ nevie a nezaujíma ho, kto ju spracuje. Pomenúva sa v minulom čase. | `OrderCreated`, `PaymentCompleted`, `PaymentFailed` |
| **Príkaz (command)** | Správa „urob toto“ pre **konkrétneho** príjemcu. Pomenúva sa rozkazovacím spôsobom. | `ProcessPayment`, `ConfirmOrder`, `CancelOrder` |
| **Topic** | Pomenovaný „kanál“ v Kafke, do ktorého sa zapisujú správy. Je to log: správy sa po prečítaní nemažú. | `orders.created`, `payments.commands`, `payments.result`, `orders.commands` |
| **Partícia (partition)** | Topic je rozdelený na partície. Poradie správ platí **len v rámci jednej partície**. | 3 partície na každom topicu |
| **Kľúč (key)** | Hodnota, podľa ktorej Kafka vyberie partíciu. Rovnaký kľúč = rovnaká partícia = zachované poradie. | vždy `orderId` |
| **Consumer group** | Skupina konzumentov, ktorá si partície topicu rozdelí. Každú správu dostane v rámci skupiny len jeden člen. | `order-service`, `payment-service`, `payment-service-dlt`, `order-process` |
| **Offset** | Poradové číslo správy v partícii. Consumer group si pamätá, po ktorý offset už spracovala. | `kafka-consumer-groups.sh --describe` |
| **Ack (commit offsetu)** | Potvrdenie, že správa je spracovaná a nemá sa doručiť znova. | `AckMode.RECORD`, `MANUAL_IMMEDIATE` |
| **DLT (dead letter topic)** | „Odkladací“ topic pre správy, ktoré sa ani po opakovaní nepodarilo spracovať. | `payments.commands.DLT`, `orders.created.DLT`, … |
| **Dual write** | Problém: zapísať do DB **a** poslať do Kafky sa nedá v jednej atomickej operácii. | rieši ho outbox |
| **Outbox** | Odchádzajúca správa sa zapíše do tabuľky v **tej istej transakcii** ako doménová zmena. Do Kafky ju neskôr pošle relay. | tabuľky `orders.outbox`, `payments.outbox`, `OutboxRelay` |
| **Inbox** | Prijatá správa sa najprv uloží do tabuľky (PK = `event_id`), až potom sa spracuje. Rieši duplicity a retry. | tabuľky `orders.inbox`, `payments.inbox`, `InboxProcessor` |
| **Idempotencia** | Opakované spracovanie tej istej správy má rovnaký výsledok ako jedno spracovanie. | inbox, `messageId` v Zeebe, `commandId` z `jobKey` |
| **At-least-once** | Záruka doručenia „aspoň raz“: nič sa nestratí, ale môže prísť duplicita. Kafka aj outbox ju dávajú. | všade, preto idempotencia |
| **Choreografia** | Služby reagujú na udalosti iných služieb, nikto celý tok neriadi. Tok je „rozsypaný“ v kóde. | pôvodná verzia (commit `ba78d05`) |
| **Orchestrácia** | Jeden orchestrátor drží stav toku a posiela príkazy. Tok je na jednom mieste. | `order-process` + BPMN v Camunde |
| **BPMN** | Business Process Model and Notation – štandardný diagram procesu, ktorý je zároveň spustiteľný. | [`order-fulfillment.bpmn`](../../order-process/src/main/resources/bpmn/order-fulfillment.bpmn) |
| **Zeebe** | Procesný engine Camundy 8. Drží stav inštancií v logu udalostí a RocksDB. | pod `camunda-0` |
| **Orchestration Cluster** | Camunda 8 v jednom balíku: Zeebe + Operate + Tasklist + REST API. | image `camunda/camunda:8.10.2` |
| **Inštancia procesu** | Jeden beh procesu – v deme jedna objednávka. | `processInstanceKey` |
| **Token** | Myslená „značka“, ktorá ukazuje, kde v diagrame sa inštancia práve nachádza. | v Operate ako zelený krúžok |
| **Job** | Úloha, ktorú Zeebe vytvorí, keď token príde na service task. Čaká, kým si ju niekto vyzdvihne. | job typy `request-payment`, `confirm-order`, `cancel-order` |
| **Job worker** | Kód, ktorý si joby daného typu vyzdvihuje, vykoná ich a ohlási dokončenie. | `RequestPaymentWorker`, `ConfirmOrderWorker`, `CancelOrderWorker` |
| **Message correlation** | Doručenie správy (message) do správnej čakajúcej inštancie. | `PaymentResult` → catch event `payment_result` |
| **Correlation key** | Hodnota, podľa ktorej Zeebe páruje správu s inštanciou. | `=orderId` |
| **Message TTL** | Ako dlho Zeebe drží publikovanú správu, ak ju zatiaľ nikto nečaká. | `eda.process.message-ttl: 1h` |
| **Incident** | Inštancia narazila na problém, ktorý sama nevyrieši (napr. jobu došli `retries`). Čaká na človeka. | Operate → Incidents |
| **Operate** | Webové UI Camundy na sledovanie inštancií, premenných a incidentov. | http://localhost:8088/operate |
| **Primárne úložisko** | Zdroj pravdy Zeebe: log udalostí + RocksDB na PVC. | PVC `data-camunda-0` |
| **Sekundárne úložisko** | Kópia dát na čítanie (Operate, search API). V deme databáza `camunda` v PostgreSQL. | DB `camunda` |
| **Exportér** | Časť Zeebe, ktorá asynchrónne kopíruje záznamy z primárneho do sekundárneho úložiska. | RDBMS exportér |
| **correlationId** | ID, ktoré putuje s objednávkou cez všetky služby (HTTP hlavička, Kafka hlavička, MDC). Nie je to correlation key! | `X-Correlation-Id`, pole `correlationId` v logoch |

> **Pozor na zámenu:** *correlation key* (`orderId`) páruje správu s inštanciou v Zeebe. *correlationId* (`demo-…`) je stopa pre logy. Sú to dve rôzne veci s podobným menom.

---

## Čo potrebuješ mať pripravené

Podrobne to rieši [kapitola 02](02_predpoklady_a_setup.md), tu je len zoznam:

- Docker Desktop so zapnutým Kubernetes a aspoň ~3 GB voľnej RAM pre kontajnery (stack `base` berie ~2,5 GB)
- `kubectl` (prichádza s Docker Desktopom)
- JDK 21 a Maven 3.9+ – len ak chceš spúšťať testy mimo Dockera
- PowerShell (Windows PowerShell 5.1 vo Windows 11), príkazy v poznámkach sú preň
- Voliteľne: Camunda Desktop Modeler (na prezeranie BPMN) a DBeaver (na SQL)

---

**Ďalej:** [01 – Čo je event-driven](01_co_je_event_driven.md)
