# 04 – Kafka v praxi

## Čo sa naučíš

- Čo je topic, partícia, offset a prečo je kľúčom každej správy `orderId`
- Ako si consumer groups delia partície a prečo má `order-process` šesť konzumentov
- Rozdiel medzi ack módmi `RECORD` a `MANUAL_IMMEDIATE` a kedy sa offset reálne potvrdí
- Ako sa pozrieť do Kafky priamo z podu: `kafka-topics.sh`, `kafka-consumer-groups.sh`, `kafka-console-consumer.sh`

---

## Teória v skratke

### 1. Kafka je log, nie fronta

Analógia: Kafka topic je **zošit s očíslovanými riadkami**. Producent píše na koniec. Čitateľ si len pamätá, na ktorom riadku skončil (offset). Prečítaním sa nič nemaže – iný čitateľ (iná consumer group) môže čítať ten istý zošit od začiatku.

```
orders.created, partícia 2:   [0] [1] [2] ... [9] [10]
                                               ^
                         order-process je tu (CURRENT-OFFSET 10 = ďalší na čítanie)
```

| Pojem | Význam | V deme |
|---|---|---|
| Topic | pomenovaný log | `orders.created`, `payments.commands`, `payments.result`, `orders.commands` + `*.DLT` |
| Partícia | topic je rozdelený na časti; každá je samostatný log | 3 partície na topic (`eda.kafka.topics.partitions`) |
| Offset | poradové číslo správy v partícii | `kafka-consumer-groups.sh --describe` |
| Kľúč | podľa neho producent vyberie partíciu (hash kľúča mod počet partícií) | vždy `orderId` |
| Replikácia | kópie partície na viacerých brokeroch | 1 broker, `replicas: 1` (demo) |

### 2. Prečo kľúč = `orderId`

Kafka garantuje poradie **len v rámci partície**. Všetky správy s rovnakým kľúčom idú do rovnakej partície, takže správy jednej objednávky sa nikdy „nepredbehnú“ v rámci jedného topicu.

Bonus: keďže všetky topicy majú 3 partície a rovnaký partitioner, objednávka skončí v **rovnakom čísle partície vo všetkých štyroch topicoch** (overíme nižšie).

### 3. Consumer group = tím čitateľov

Consumer group si partície topicu **rozdelí**: každú partíciu číta v danej chvíli práve jeden člen skupiny. Viac členov ako partícií nemá zmysel (zvyšní by nič nerobili).

| Group | Číta | Členov | Prečo toľko |
|---|---|---|---|
| `order-service` | `orders.commands` | 3 | `concurrency: 3` |
| `payment-service` | `payments.commands` | 3 | `concurrency: 3` |
| `payment-service-dlt` | `payments.commands.DLT` | 1 | DLT listener len loguje |
| `order-process` | `orders.created` **a** `payments.result` | 6 | 2 listenery × `concurrency: 3` |

### 4. Ack módy: kedy sa offset potvrdí

Potvrdenie offsetu (commit) hovorí Kafke: „po tento riadok mám hotovo, znova mi to nedávaj“. `enable-auto-commit` je vo všetkých službách **vypnutý** – offset potvrdzuje Spring Kafka podľa ack módu.

| Služba | Ack mód | Kedy sa commitne offset |
|---|---|---|
| order-service | `RECORD` | automaticky po **úspešnom** návrate z listenera (správa uložená v inboxe) |
| order-process | `RECORD` | automaticky po úspešnom `publishMessage` do Zeebe |
| payment-service | `MANUAL_IMMEDIATE` | keď listener sám zavolá `ack.acknowledge()` – hneď, synchrónne |

Výsledok je v oboch prípadoch rovnaký (commit až po uložení), `payment-service` len ukazuje druhý spôsob. Pri `MANUAL_IMMEDIATE` je jedna pasca: keď správu pošle do DLT error handler, listener ju nepotvrdil – preto má `payment-service` v `KafkaConfig` navyše `handler.setCommitRecovered(true)`.

Ak listener vyhodí výjimku, offset sa **nepotvrdí** a `DefaultErrorHandler` správu skúsi znova (3× s backoffom 0,5 s → 1 s → 2 s), potom ju pošle do `<topic>.DLT`.

### 5. JSON bez Java typov

Producenti majú `spring.json.add.type.headers: false` – do správy sa **nepridáva** hlavička `__TypeId__` s názvom Java triedy. Konzument si typ určí sám (`spring.json.value.default.type`). Pri topicoch s viacerými typmi (`payments.result`, `orders.commands`) rozhoduje pole `"type"` v JSON (`@JsonTypeInfo`). Vďaka tomu konzument nezávisí od balíčkov producenta.

---

## Krok po kroku

Kafka CLI nástroje sú v image `apache/kafka` v `/opt/kafka/bin/`. Spúšťame ich cez `kubectl exec` priamo v pode Kafky, takže na Windows nič neinštaluješ.

> V **Git Bash** treba pred tieto príkazy dať `MSYS_NO_PATHCONV=1`, inak sa `/opt/...` prepíše na Windows cestu. V PowerShelli tento problém nie je.

### 1. Zoznam topicov

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

```
__consumer_offsets
orders.commands
orders.commands.DLT
orders.created
orders.created.DLT
payments.commands
payments.commands.DLT
payments.result
payments.result.DLT
```

- `__consumer_offsets` je interný topic, kde Kafka ukladá offsety consumer groups.
- Všetky ostatné vytvorili **služby** pri štarte (`KafkaAdmin.NewTopics` v `KafkaConfig#edaTopics`). Každá služba zakladá topicy, ktoré produkuje alebo konzumuje, a DLT svojich konzumentov: `payments.commands.DLT` (payment-service), `orders.commands.DLT` (order-service), `orders.created.DLT` a `payments.result.DLT` (order-process). Broker má `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, takže preklep v názve topicu skončí chybou, nie novým prázdnym topicom.

### 2. Detail topicu

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.created
```

```
Topic: orders.created	TopicId: AQEp_oiVS7aEBqp570ILjg	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: orders.created	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: orders.created	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
	Topic: orders.created	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
```

3 partície, 1 replika, jediný broker (ID 1) je leader všetkých.

### 3. Consumer groups a lag

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

```
payment-service
order-process
payment-service-dlt
order-service
```

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-process
```

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID                                 HOST         CLIENT-ID
order-process   payments.result 0          6               6               0     consumer-order-process-4-bd5fac1b-…        /10.1.1.166  consumer-order-process-4
order-process   orders.created  2          10              10              0     consumer-order-process-3-770da087-…        /10.1.1.166  consumer-order-process-3
order-process   orders.created  0          6               6               0     consumer-order-process-1-a1f6b5ce-…        /10.1.1.166  consumer-order-process-1
order-process   payments.result 2          9               9               0     consumer-order-process-6-7c6a7d46-…        /10.1.1.166  consumer-order-process-6
order-process   orders.created  1          4               4               0     consumer-order-process-2-b80e4d28-…        /10.1.1.166  consumer-order-process-2
order-process   payments.result 1          4               4               0     consumer-order-process-5-93ac4ee3-…        /10.1.1.166  consumer-order-process-5
The consumer rebalance protocol (KIP-848) is production-ready! ...
```

Ako to čítať:

- `CURRENT-OFFSET` = po ktorý offset má skupina potvrdené, `LOG-END-OFFSET` = koľko správ v partícii je, `LAG` = rozdiel = **koľko čaká na spracovanie**. Lag 0 = všetko prečítané.
- Šesť konzumentov (`consumer-order-process-1` až `-6`), každý má jednu partíciu: 2 listenery × 3 vlákna.
- Súčet `LOG-END-OFFSET` na `orders.created` je 6 + 4 + 10 = 20 objednávok; na `payments.result` 6 + 4 + 9 = 19 – jedna objednávka (poison 666) výsledok platby nikdy nedostala.
- Rozdelenie objednávok do partícií nie je rovnomerné (6/4/10) – rozhoduje hash `orderId`.
- Posledný riadok je len informačná hláška Kafka 4.x o novom rebalance protokole, demo ho nepoužíva.

Všetky skupiny naraz:

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

```
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG ...
order-service   orders.commands 2          9               9               0
order-service   orders.commands 0          6               6               0
order-service   orders.commands 1          4               4               0

GROUP           TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG ...
payment-service payments.commands 1          4               4               0
payment-service payments.commands 0          6               6               0
payment-service payments.commands 2          10              10              0

GROUP               TOPIC                 PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG ...
payment-service-dlt payments.commands.DLT 0          -               0               -
payment-service-dlt payments.commands.DLT 1          -               0               -
payment-service-dlt payments.commands.DLT 2          1               1               0
...
```

Lag je všade 0 – aj pri poison správe. Prečo? Listener ju **uložil do inboxu** a hneď potvrdil offset. Retry a DLT sa dejú až potom, mimo Kafky ([kapitola 05](05_outbox_a_inbox.md)). `-` v `CURRENT-OFFSET` znamená, že skupina z tej partície ešte nič nepotvrdila (je prázdna).

### 4. Prečítaj správy

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning --max-messages 2 --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.headers=true --timeout-ms 5000
```

```
Partition:1	X-Correlation-Id:demo-1791439489-6eb2-4	c56494b3-684b-4070-9b37-94760de5d1ce	{"amount": 387.67, "eventId": "861a018a-0e91-42d9-9a4d-8d9fd7d0f6b9", "orderId": "c56494b3-…", "currency": "CZK", "timestamp": "2026-10-08T06:04:49.929317308Z", "customerId": "customer-4", "correlationId": "demo-1791439489-6eb2-4"}
Partition:1	X-Correlation-Id:demo-1791439490-6efd-7	56d7a578-a92b-4524-8f46-00deaf7c077b	{"amount": 285.36, ...}
Processed a total of 2 messages
```

- Formát riadku: partícia, hlavičky, **kľúč** (`orderId`), hodnota (JSON).
- Hlavička `X-Correlation-Id` nesie stopu objednávky ([kapitola 11](11_observabilita.md)).
- Prečo sú polia v JSON abecedne a s medzerami? Správa prešla cez **outbox**, kde je payload uložený ako PostgreSQL `JSONB`, a ten si JSON normalizuje. Správy od `order-process` (bez outboxu) sú kompaktné v poradí polí recordu – porovnaj s ďalšími dvoma príkazmi.
- `--from-beginning` + vlastná (náhodná) consumer group konzoly – **neovplyvní** offsety služieb.

```powershell
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payments.result --from-beginning --max-messages 1 --formatter-property print.key=true --timeout-ms 5000
kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.commands --from-beginning --max-messages 1 --formatter-property print.key=true --timeout-ms 5000
```

```
e3bf1ece-250c-4201-b422-e4e771aa2613	{"type": "PaymentFailed", "reason": "Payment declined by simulated gateway", "eventId": "d8b251be-…", "orderId": "e3bf1ece-…", "timestamp": "2026-10-08T06:04:51.041114396Z", "correlationId": "demo-1791439490-2e02-5"}
751b4da4-be21-4cf8-860a-6f8d8b632ad3	{"type":"ConfirmOrder","eventId":"60b831d4-65cc-3389-91ac-b42b92efa6e9","timestamp":"2026-10-08T06:04:51.643211377Z","correlationId":"demo-1791439489-073c-1","orderId":"751b4da4-…","paymentId":"60ce3416-…"}
```

Na oboch topicoch s viacerými typmi je diskriminátor `"type"`. A všimni si `eventId` príkazu: `…-65cc-3389-…` – trojka na začiatku tretej skupiny znamená **UUID verzie 3** (odvodené z mena, nie náhodné). To je `commandId` odvodený z `jobKey` ([kapitola 08](08_most_zeebe_kafka.md)).

### 5. Rovnaký kľúč = rovnaká partícia vo všetkých topicoch

Objednávka `3101528e-…` z kapitoly 01 vo všetkých štyroch topicoch:

```powershell
foreach ($t in 'orders.created','payments.commands','payments.result','orders.commands') {
  $line = kubectl -n eda-demo exec deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $t --from-beginning --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.offset=true --timeout-ms 4000 2>$null | Select-String '3101528e-e2ce-4a55-bfa6-a7790b46e89e' | Select-Object -First 1
  '{0,-18} {1}' -f $t, $line.Line.Substring(0, 75)
}
```

```
orders.created     Partition:2	Offset:9	3101528e-e2ce-4a55-bfa6-a7790b46e89e	{"amount": 99.90,
payments.commands  Partition:2	Offset:9	3101528e-e2ce-4a55-bfa6-a7790b46e89e	{"eventId":"0ebf6
payments.result    Partition:2	Offset:8	3101528e-e2ce-4a55-bfa6-a7790b46e89e	{"type": "Payment
orders.commands    Partition:2	Offset:8	3101528e-e2ce-4a55-bfa6-a7790b46e89e	{"type":"ConfirmO
```

Partícia 2 všade. (`2>$null` potlačí hlášku „Processed a total of …“ zo stderr.)

---

## Kód z repa

| Súbor | Čo v ňom je |
|---|---|
| [`order-process/.../config/KafkaConfig.java`](../../order-process/src/main/java/cz/demo/eda/process/config/KafkaConfig.java) | `edaTopics` (vytvorí 4 topicy + 2 DLT), `kafkaErrorHandler` (`DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `ExponentialBackOffWithMaxRetries`) |
| [`order-process/src/main/resources/application.yml`](../../order-process/src/main/resources/application.yml) | `acks: all`, `enable.idempotence: true`, `enable-auto-commit: false`, `ack-mode: record`, `concurrency: 3`, `ErrorHandlingDeserializer` |
| [`payment-service/.../config/KafkaConfig.java`](../../payment-service/src/main/java/cz/demo/eda/payment/config/KafkaConfig.java) | to isté + `setCommitRecovered(true)` kvôli `MANUAL_IMMEDIATE` |
| [`payment-service/.../messaging/ProcessPaymentListener.java`](../../payment-service/src/main/java/cz/demo/eda/payment/messaging/ProcessPaymentListener.java) | `ack.acknowledge()` po uložení do inboxu |
| [`order-service/.../messaging/OrderCommandListener.java`](../../order-service/src/main/java/cz/demo/eda/order/messaging/OrderCommandListener.java) | bez `Acknowledgment` – pri `RECORD` sa potvrdí návratom z metódy |
| [`k8s/base/kafka/kafka.yaml`](../../k8s/base/kafka/kafka.yaml) | KRaft (broker + controller v jednom procese), `KAFKA_AUTO_CREATE_TOPICS_ENABLE`, `emptyDir` |

Dve dôležité nastavenia producenta:

- `acks: all` – broker potvrdí zápis, až keď ho majú všetky in-sync repliky (tu jedna).
- `enable.idempotence: true` – producent pri retry neposiela duplicity a drží poradie v partícii.

A jedno konzumenta: `ErrorHandlingDeserializer`. Keď príde nečitateľný JSON, deserializácia nespadne v `poll()` (čo by znamenalo nekonečnú slučku), ale chyba sa odovzdá error handleru, ktorý správu pošle do DLT. DLT recoverer pritom používa `ByteArraySerializer` pre `byte[]` hodnoty, aby sa pôvodné bajty nezakódovali do base64.

`DLT` má **rovnaký počet partícií** ako zdrojový topic: recoverer (`dltPartition`) posiela správu do rovnakého čísla partície.

---

## Bez Camundy by to vyzeralo takto

V choreografii (`ba78d05`) boli len dva hlavné topicy a dve consumer groups:

| Topic | Producent → konzument |
|---|---|
| `orders.created` | order-service → **payment-service** |
| `payments.result` | payment-service → **order-service** |

Teraz sú štyri a oba konzumuje `order-process`. Pribudli **príkazové** topicy (`payments.commands`, `orders.commands`). Tvar správ sa takmer nezmenil – `ProcessPayment` nesie to isté ako kedysi `OrderCreated` (bez `customerId`).

---

## Časté chyby

| Príznak | Príčina | Riešenie |
|---|---|---|
| `kafka-console-consumer.sh` v Git Bash: `No such file or directory` s cestou `C:/Program Files/Git/opt/...` | MSYS prepisuje cesty | `MSYS_NO_PATHCONV=1` pred príkaz, alebo použi PowerShell |
| Consumer nič nevypíše | Bez `--from-beginning` číta len nové správy | Pridaj `--from-beginning`; `--timeout-ms` aby skončil |
| Lag rastie a neklesá | Consumer nebeží (pod dole) alebo stále padá a čaká na retry | `kubectl get pods`, logy služby (`Delivery attempt`) |
| Služba pri štarte padá na `fail-fast` KafkaAdmin | Kafka nebeží, topicy sa nedajú vytvoriť | initContainer čaká na `kafka:9092`; skontroluj pod Kafky |
| Po reštarte podu Kafky zmizli topicy aj správy | `emptyDir` – dáta žijú len s podom | Obmedzenie dema; reštartuj služby (vytvoria topicy), stratené správy sú preč |
| Správa skončí v DLT s `kafka_dlt-exception-fqcn` … `DeserializationException` | Nečitateľný JSON alebo chýba `type` | Oprav producenta; obsah v DLT ostane na analýzu |

---

## Otázky na zopakovanie

**Prečo je kľúčom správ `orderId`?**
Rovnaký kľúč = rovnaká partícia = zachované poradie správ jednej objednávky. Zároveň sa správy rôznych objednávok rozložia na partície a spracúvajú paralelne.

**Čo je lag a prečo je pri poison správe 0?**
Lag = počet nepotvrdených správ v partícii. Pri poison je 0, lebo listener správu uloží do inboxu a hneď potvrdí; opakovanie beží v inboxe, nie v Kafke.

**Aký je rozdiel medzi `RECORD` a `MANUAL_IMMEDIATE`?**
`RECORD` potvrdí offset automaticky po úspešnom návrate listenera. `MANUAL_IMMEDIATE` čaká, kým kód zavolá `ack.acknowledge()`, a potvrdí hneď synchrónne.

**Prečo `order-process` má šesť konzumentov?**
Má dva `@KafkaListener` (na `orders.created` a `payments.result`), každý s `concurrency: 3`, v jednej group `order-process`.

**Prečo sú vypnuté `__TypeId__` hlavičky?**
Aby konzument nezávisel od Java tried producenta. Typ určuje konzument (default typ alebo pole `type`).

## Vyskúšaj si

1. Pošli 1 objednávku a hneď spusti `--describe --group payment-service`. **Očakávanie:** `LOG-END-OFFSET` v jednej partícii stúpol o 1 a lag je 0 (alebo na okamih 1).
2. Prečítaj `orders.commands` s `print.headers=true` a nájdi hlavičku `X-Correlation-Id`. **Očakávanie:** rovnaká hodnota ako `correlationId` v tele správy – posiela ju `CommandPublisher`.
3. Vyber si ľubovoľné `orderId` a overiť, že vo všetkých štyroch topicoch je v tej istej partícii (cyklus z kroku 5). **Očakávanie:** áno, vždy.

---

**Ďalej:** [05 – Outbox a inbox](05_outbox_a_inbox.md)
