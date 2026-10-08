# Brief: tutoriál docs/poznamky/ – „Event-driven a Camunda po lopate“ (samoštúdium)

## Vzor
Štýl, štruktúra a tón presne podľa Liferay poznámok autora (prečítaj si aspoň tieto, cez WebFetch):
- https://raw.githubusercontent.com/eriskooo/liferay-demo/master/poznamky/00_osnova.md
- https://raw.githubusercontent.com/eriskooo/liferay-demo/master/poznamky/05_osgi_a_gogo_shell.md
- https://raw.githubusercontent.com/eriskooo/liferay-demo/master/poznamky/14_zhrnutie.md
Jazyk: **slovenčina** (ako vzor). Technické termíny anglicky, kde je to bežné. Konverzačne, ale presne („po lopate“), analógie, vždy PREČO.

## Účel
Samoštúdium (NIE príprava na pohovor): čitateľ chce pochopiť event-driven architektúru a stavový automat v Camunde na tomto repe. Namiesto „Otázky na pohovor“ má každá kapitola „Otázky na zopakovanie“ (otázka + krátka odpoveď) a „Vyskúšaj si“ (malý experiment s očakávaným výsledkom).

## Štruktúra každej kapitoly (01–11)
1. **Čo sa naučíš** – 2 až 4 body
2. **Teória v skratke** – pár odsekov, analógie, tabuľky; mermaid diagram tam, kde pomáha
3. **Krok po kroku** – PowerShell príkazy (Windows 11, Windows PowerShell 5.1) + **očakávaný výstup** (reálny, skrátený)
4. **Kód z repa** – konkrétne súbory s relatívnymi odkazmi (`../../order-process/...`) a vysvetlením
5. **Bez Camundy by to vyzeralo takto** – porovnanie s pôvodnou choreografiou (kde to dáva zmysel; pôvodný stav je v git histórii pred touto zmenou: commit ba78d05)
6. **Časté chyby** – tabuľka Príznak → Príčina → Riešenie
7. **Otázky na zopakovanie** + **Vyskúšaj si**

## Kapitoly (súbory v docs/poznamky/)
| # | súbor | obsah |
|---|---|---|
| 00 | 00_osnova.md | cieľ, ako čítať, štruktúra kapitol, mapa kapitol (tabuľka), slovník pojmov (event, príkaz/command, topic, partícia, kľúč, consumer group, offset, ack, DLT, dual write, outbox, inbox, idempotencia, at-least-once, choreografia, orchestrácia, BPMN, Zeebe, Orchestration Cluster, job, job worker, message correlation, correlation key, message TTL, incident, Operate, primárne/sekundárne úložisko, exportér, correlationId), čo treba mať pripravené |
| 01 | 01_co_je_event_driven.md | eventy vs. príkazy, choreografia vs. orchestrácia, prečo stavový automat, prehľad architektúry repa |
| 02 | 02_predpoklady_a_setup.md | Docker Desktop + Kubernetes, JDK 21, Maven, RAM, orientácia v repe |
| 03 | 03_spustenie_stacku.md | teardown/build-images/deploy/port-forward/send-orders, prvá objednávka, Operate (demo/demo, http://localhost:8088/operate), Postgres na 15432 |
| 04 | 04_kafka_v_praxi.md | topicy, partície, kľúč orderId, consumer groups, ack módy (RECORD vs MANUAL_IMMEDIATE), Kafka CLI v pode (kafka-topics, kafka-consumer-groups, kafka-console-consumer) |
| 05 | 05_outbox_a_inbox.md | dual write, OutboxRelay, inbox deduplikácia, retry s backoffom, DLT, SKIP LOCKED; SQL nad tabuľkami (DBeaver/psql) |
| 06 | 06_camunda_a_zeebe.md | čo je Zeebe, pull model (Zeebe nikoho nevolá), job streaming, primárne vs. sekundárne úložisko, exportér, Operate |
| 07 | 07_bpmn_proces.md | order-fulfillment.bpmn element po elemente, Desktop Modeler, token, stavový automat, gateway, correlation key |
| 08 | 08_most_zeebe_kafka.md | ProcessGateway, listenery, workery, CommandPublisher, messageId=eventId, commandId z jobKey, TTL 1h a prečo |
| 09 | 09_chybove_scenare.md | poison 666 (inbox retry → DLT, inštancia čaká), incident, duplicity, výpadok Zeebe/Kafky, predbehnutá správa, TTL |
| 10 | 10_testovanie.md | unit testy, Testcontainers, Camunda Process Test (OrderProcessIntegrationTest), ako spustiť |
| 11 | 11_observabilita.md | correlationId v hlavičke/MDC, ECS JSON logy, overlay monitoring/logging (Grafana, Kibana) – len popis, ak nie je nasadené |
| 12 | 12_cvicenia.md | 8–12 cvičení so zadaním, nápovedou a riešením (napr. pridať timer boundary event na Payment result; poslať duplicitný PaymentResult cez kafka-console-producer; zastaviť order-process (scale 0) a sledovať, čo sa stane; nájsť cestu objednávky podľa correlationId; pridať krok do BPMN) |
| 13 | 13_zhrnutie.md | 10 hlavných bodov |

## Pravidlá overovania (dôležité)
- Ako vo vzore: každý príkaz v kapitolách reálne spusti a výstup skopíruj (skrátene). Ak niečo nejde overiť, kapitola to výslovne uvedie.
- Stack už beží v docker-desktop Kubernetes (namespace eda-demo), port-forward beží: order-service :8080, payment-service :8081, order-process :8082, Operate :8088, Postgres :15432 (svc/postgres). Ak niektorý forward nejde, spusti `kubectl -n eda-demo port-forward svc/<x> <local>:<remote>` na pozadí.
- POVOLENÉ: kubectl get/describe/logs/exec (čítanie, Kafka CLI v pode kafka, psql v pode postgres), curl, scripts/send-orders, Camunda REST API (`POST localhost:8088/v2/process-instances/search` atď.), dočasné `kubectl scale` s **povinným vrátením** na pôvodný počet replík a overením Ready.
- ZAKÁZANÉ: teardown, `kubectl delete`, deploy/apply, zmena manifestov alebo kódu, akékoľvek git zápisy (add/commit/...). Kapitolu 03 (teardown/deploy) dokumentuj z už zachytených výstupov: .superpowers/sdd/2026-10-08-camunda-orchestration/deploy.log, statuses.txt, orders.txt, progress.md (sekcia K8s verification).
- Fakty ber z kódu a README (README.md je aktuálne a zreview-ované). Nevymýšľaj funkcie, ktoré repo nemá.
- Príkazy píš pre PowerShell 5.1 (žiadne `&&`, `curl` → `curl.exe`); pre bash variantu skriptov stačí zmienka.

## Výstup
Súbory docs/poznamky/00…13 .md. Do README.md pridaj krátku sekciu/odkaz „Poznámky na samoštúdium → docs/poznamky/00_osnova.md“ (jediná povolená zmena mimo docs/poznamky).
