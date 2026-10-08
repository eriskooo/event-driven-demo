# 13 – Zhrnutie: 10 hlavných bodov

Desať najdôležitejších vecí z kapitol 01–12. Pri každom bode je odkaz na kapitolu s podrobnosťami.

---

## 1. Event hovorí, čo sa stalo; príkaz hovorí, čo sa má stať

`OrderCreated`, `PaymentCompleted`, `PaymentFailed` sú **eventy** – odosielateľa nezaujíma, kto na ne zareaguje. `ProcessPayment`, `ConfirmOrder`, `CancelOrder` sú **príkazy** pre konkrétneho príjemcu. Doménové služby publikujú eventy a vykonávajú príkazy, orchestrátor eventy počúva a príkazy posiela.

→ [kapitola 01](01_co_je_event_driven.md)

## 2. Orchestrácia dáva toku jedno miesto a jeden stav

V choreografii (`ba78d05`) bol tok rozsypaný v listeneroch dvoch služieb a jeho stav nebol nikde. S Camundou je tok **BPMN diagram** a stav každej objednávky je **token v Zeebe** – viditeľný v Operate aj cez REST API. Cena: ďalší pod, druhé úložisko a nové chybové stavy (Zeebe dole).

→ [kapitola 01](01_co_je_event_driven.md), [kapitola 07](07_bpmn_proces.md)

## 3. Kafka je log s partíciami; kľúč `orderId` drží poradie

Poradie platí len v rámci partície. Rovnaký kľúč = rovnaká partícia (v deme dokonca rovnaké číslo partície vo všetkých štyroch topicoch). Consumer group si partície delí, offset sa potvrdzuje až po úspešnom spracovaní (`RECORD` alebo `MANUAL_IMMEDIATE`).

→ [kapitola 04](04_kafka_v_praxi.md)

## 4. Kafka doručuje aspoň raz – preto je všetko idempotentné

| Kde hrozí duplicita | Čo ju zachytí |
|---|---|
| doménová služba dostane príkaz dvakrát | **inbox**, PK `event_id` |
| Zeebe dostane `OrderCreated` / `PaymentResult` dvakrát | **`messageId = eventId`** → `ALREADY_EXISTS` = úspech |
| worker zopakuje job | **`eventId` príkazu = f(`jobKey`)** → inbox príjemcu |

→ [kapitola 05](05_outbox_a_inbox.md), [kapitola 08](08_most_zeebe_kafka.md)

## 5. Outbox rieši dual write, inbox rieši duplicity a retry

**Outbox**: doménová zmena a odchádzajúca správa v jednej DB transakcii, `OutboxRelay` ju neskôr pošle (at-least-once). **Inbox**: listener len uloží správu a potvrdí offset, `InboxProcessor` ju spracuje v transakcii s retry 0,5 s → 1 s → 2 s a po 4 pokusoch ju pošle do DLT. `SKIP LOCKED` dovoľuje viac replík.

→ [kapitola 05](05_outbox_a_inbox.md)

## 6. Zeebe nikoho nevolá

`order-process` je klient: listenery **publikujú správy**, workery si **aktivujú joby** (long polling – job streaming v deme nie je zapnutý) a hlásia ich dokončenie. Keď worker nebeží, joby počkajú. Zeebe nemusí poznať žiadnu službu.

→ [kapitola 06](06_camunda_a_zeebe.md)

## 7. Primárne úložisko je pravda, sekundárne je výpis

Log + RocksDB na PVC = zdroj pravdy (inštancie prežijú reštart podu). DB `camunda` v PostgreSQL plní exportér asynchrónne, čítajú ju Operate a search API – je **eventually consistent**. Nikdy na nej nestav logiku.

→ [kapitola 06](06_camunda_a_zeebe.md)

## 8. Message correlation a TTL

`PaymentResult` sa páruje s inštanciou podľa **correlation key `=orderId`**. Ak príde skôr, ako proces čaká, Zeebe ho **podrží počas TTL** (1 h). TTL musí byť dlhšie ako timeout jobu (5 min) + čas na incident, inak by predbehnutý výsledok vypršal. TTL je zároveň okno deduplikácie `messageId`.

→ [kapitola 07](07_bpmn_proces.md), [kapitola 08](08_most_zeebe_kafka.md)

## 9. Každá chyba má svoje miesto a svoju opravu

| Chyba | Kde skončí | Oprava |
|---|---|---|
| technická chyba platby (666) | `payments.commands.DLT`, inštancia čaká na `payment_result` | poslať procesu `PaymentFailed` s novým `eventId` |
| Zeebe dole | `orders.created.DLT`, objednávka bez inštancie | redrive s **pôvodným** `eventId` (až keď gRPC naozaj odpovedá) |
| `order-process` dole | lag v Kafke | nič – po nábehu dobehne sám |
| Kafka dole pri odosielaní príkazu | incident v Operate | opraviť, **Retry** v Operate |

→ [kapitola 09](09_chybove_scenare.md)

## 10. Cestu objednávky vidíš z troch strán

- **Logy** podľa `correlationId` (ECS JSON, MDC cez HTTP filter, Kafka interceptor, inbox a `CorrelationScope`),
- **Operate / REST API** – kde je token, aké sú premenné, ako dlho čakal,
- **metriky** – `eda_*`, `camunda_client_worker_*`, `zeebe_*`.

A testy overia celý tok bez clustra: Camunda Process Test + Testcontainers (232 testov, 0 zlyhaní).

→ [kapitola 10](10_testovanie.md), [kapitola 11](11_observabilita.md)

---

## Hlavná myšlienka

Event-driven systém nie je spoľahlivý preto, že sa nič nepokazí, ale preto, že **každá správa má ID a každý krok sa dá bezpečne zopakovať**. Camunda k tomu pridáva to, čo choreografii chýba: **stav toku na jednom mieste** – takže keď sa niečo pokazí, vieš, kde presne to stojí.
