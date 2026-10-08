# 12 – Cvičenia

Desať cvičení od jednoduchých po náročné. Každé má **zadanie**, **nápovedu** (prečítaj si ju, až keď sa zasekneš) a **riešenie**. Pri každom je uvedené, či bolo riešenie pri písaní overené naživo.

Pred začiatkom: stack beží, port-forward beží ([kapitola 03](03_spustenie_stacku.md)).

| # | Cvičenie | Náročnosť | Overené naživo |
|---|---|---|---|
| 1 | Nájdi cestu objednávky podľa `correlationId` | ★ | áno |
| 2 | Nájdi inštanciu procesu podľa `orderId` | ★ | áno |
| 3 | Pošli duplicitný `PaymentResult` | ★★ | áno |
| 4 | Prepočítaj `eventId` príkazu z `jobKey` | ★★ | áno |
| 5 | Vypni `order-process` a sleduj lag | ★★ | áno |
| 6 | Zachráň poison objednávku | ★★ | áno |
| 7 | Výpadok Zeebe a redrive z DLT | ★★★ | áno |
| 8 | `SKIP LOCKED` v dvoch session | ★★ | áno |
| 9 | Timer na čakanie na platbu | ★★★ | **nie** (zmena BPMN) |
| 10 | Nový krok v BPMN | ★★★ | **nie** (zmena kódu) |

---

## Cvičenie 1 – Cesta objednávky podľa `correlationId`

**Zadanie:** Pošli objednávku s vlastným `X-Correlation-Id` a vypíš všetky logy všetkých troch služieb, ktoré k nej patria, zoradené podľa času.

**Nápoveda:** Logy sú ECS JSON. `kubectl logs deploy/<x>`, `Select-String -SimpleMatch`, `ConvertFrom-Json`, `Sort-Object '@timestamp'`.

**Riešenie:**

```powershell
curl.exe -s -X POST http://localhost:8080/orders -H "Content-Type: application/json" -H "X-Correlation-Id: cvicenie-1" -d '{\"customerId\":\"c1\",\"amount\":25.00,\"currency\":\"CZK\"}'
Start-Sleep -Seconds 3
$cid = 'cvicenie-1'
$all = foreach ($d in 'order-service','order-process','payment-service') {
  kubectl -n eda-demo logs deploy/$d --since=10m 2>$null | Select-String -SimpleMatch $cid | ForEach-Object { $_.Line | ConvertFrom-Json }
}
$all | Sort-Object '@timestamp' | ForEach-Object { '{0}  {1,-15}  {2}' -f $_.'@timestamp'.Substring(11,12), $_.service.name, $_.message }
```

Výstup (overené, správy skrátené):

```
06:48:16.433  order-service    Order ab9a7146-… created for customer c1 amount 25.00 CZK
06:48:16.597  order-service    Published event c0c8826e-… for order ab9a7146-… to orders.created
06:48:16.625  order-process    Message OrderCreated published for ab9a7146-… (messageId c0c8826e-…)
06:48:16.625  order-process    Process order-fulfillment requested for order ab9a7146-…
06:48:16.645  order-process    ProcessPayment e329d612-4828-34e8-… sent for order ab9a7146-…
06:48:16.653  payment-service  ProcessPayment e329d612-4828-34e8-… for order ab9a7146-… stored in inbox
06:48:16.756  payment-service  Processing payment for order ab9a7146-… amount 25.0 CZK
06:48:16.759  payment-service  Payment for order ab9a7146-… finished with PaymentCompleted
06:48:16.772  payment-service  Published event 116bf05d-… for order ab9a7146-… to payments.result
06:48:16.794  order-process    Message PaymentResult published for ab9a7146-… (messageId 116bf05d-…)
06:48:16.811  order-process    ConfirmOrder 17fa15fc-220e-3899-… sent for order ab9a7146-…
06:48:16.819  order-service    Received ConfirmOrder 17fa15fc-220e-3899-… for order ab9a7146-… stored in inbox
06:48:17.089  order-service    Order ab9a7146-… status changed PENDING_PAYMENT -> PAID
```

13 riadkov. Pri zamietnutej platbe `CancelOrder` namiesto `ConfirmOrder` a `PENDING_PAYMENT -> PAYMENT_FAILED`.

---

## Cvičenie 2 – Inštancia procesu podľa `orderId`

**Zadanie:** Máš `orderId` z odpovede `POST /orders`. Nájdi `processInstanceKey` a vypíš cestu tokenu.

**Nápoveda:** `POST /v2/variables/search` s filtrom `name` a `value` (hodnota je JSON!), potom `POST /v2/element-instances/search`.

**Riešenie:**

```powershell
$orderId = '14ef2d96-a49f-4c36-baa1-6e55b1731aa1'
$v = Invoke-RestMethod -Method Post -Uri http://localhost:8088/v2/variables/search -ContentType 'application/json' -Body ('{"filter":{"name":"orderId","value":"\"' + $orderId + '\""}}')
$k = $v.items[0].processInstanceKey
$e = Invoke-RestMethod -Method Post -Uri http://localhost:8088/v2/element-instances/search -ContentType 'application/json' -Body ('{"filter":{"processInstanceKey":"' + $k + '"},"sort":[{"field":"startDate"}]}')
$e.items | Select-Object elementId, state
```

Overené v [kapitole 06](06_camunda_a_zeebe.md), kroky 3 a 4: `processInstanceKey 2251799813686708`, cesta `order_created → … → order_confirmed`.

---

## Cvičenie 3 – Duplicitný `PaymentResult`

**Zadanie:** Pošli do `payments.result` presnú kópiu výsledku platby už dokončenej objednávky. Dokáž, že sa nič nestalo.

**Nápoveda:** `eventId` výsledku nájdeš v logu payment-service (`Published event … to payments.result`) alebo cez `kafka-console-consumer`. `kafka-console-producer.sh` s `parse.key=true`, `key.separator='|'`, `kubectl exec -i`.

**Riešenie:**

```powershell
$msg = '3101528e-e2ce-4a55-bfa6-a7790b46e89e|{"type":"PaymentCompleted","eventId":"342cdce7-fe71-40ad-8a64-77de498ee541","timestamp":"2026-10-08T06:21:28.848Z","correlationId":"tutorial-1","orderId":"3101528e-e2ce-4a55-bfa6-a7790b46e89e","paymentId":"dca41277-ed8a-430b-937a-661d7bcf84c7","amount":99.9}'
$msg | kubectl -n eda-demo exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.result --reader-property parse.key=true --reader-property key.separator='|'
Start-Sleep -Seconds 3
kubectl -n eda-demo logs deploy/order-process --since=1m 2>$null | Select-String 'Duplicate' | ForEach-Object { ($_.Line | ConvertFrom-Json).message }
```

```
Duplicate message PaymentResult for 3101528e-e2ce-4a55-bfa6-a7790b46e89e (messageId 342cdce7-fe71-40ad-8a64-77de498ee541) skipped
```

Prečo: `messageId = eventId`, Zeebe správu s týmto ID drží 1 h → `ALREADY_EXISTS` → `ProcessGateway` vráti `false`. Pozor: funguje to len **do hodiny** od pôvodnej správy (TTL). Neskôr by Zeebe správu prijal, ale keďže na ňu nikto nečaká, nič by sa nestalo.

---

## Cvičenie 4 – `eventId` príkazu z `jobKey`

**Zadanie:** Z logu `order-process` vezmi `jobKey` jedného `ProcessPayment` a v PowerShelli vypočítaj jeho `eventId` bez pozerania do Kafky.

**Nápoveda:** `UUID.nameUUIDFromBytes("job-" + jobKey)` = MD5 + verzia 3 (`byte[6]`) + IETF variant (`byte[8]`).

**Riešenie:** funkcia `Get-CommandId` z [kapitoly 08](08_most_zeebe_kafka.md), krok 2. Overené: `jobKey 2251799813686721` → `2c32cb6a-3d1d-333a-899a-ec5f9b478541`, presne `eventId` z logu.

---

## Cvičenie 5 – `order-process` vypnutý

**Zadanie:** Vypni orchestrátor, pošli objednávku, ukáž, kde správa čaká, a obnov stav.

**Nápoveda:** `kubectl scale --replicas=0`, `kafka-consumer-groups.sh --describe --group order-process`. **Vždy** obnov pôvodný počet replík a over Ready.

**Riešenie:** postup v [kapitole 09](09_chybove_scenare.md), krok 4. Overené: `LAG 1` na `orders.created`, objednávka `PENDING_PAYMENT`; po `scale --replicas=1` a `rollout status` → `PAID`. Po obnove znova spusti port-forward na 8082.

Kontrolné otázky: Prečo sa nič nestratilo? (Offset sa nepotvrdil, správa je v Kafke.) Prečo nemusíš nič „redrivovať“? (Consumer pokračuje od posledného potvrdeného offsetu.)

---

## Cvičenie 6 – Záchrana poison objednávky

**Zadanie:** Pošli objednávku so sumou 666. Keď skončí v DLT, dokonči ju tak, aby bola `PAYMENT_FAILED` a inštancia procesu `COMPLETED` – **bez** zásahu do DB a bez Operate.

**Nápoveda:** Na čo inštancia čaká? Kto by jej to normálne poslal? Akú správu a s akým `eventId`?

**Riešenie:** pošli `PaymentFailed` s **novým** `eventId` do `payments.result` ([kapitola 09](09_chybove_scenare.md), krok 2):

```powershell
$order = '<orderId poison objednávky>'
$json = @{ type = 'PaymentFailed'; eventId = [guid]::NewGuid().ToString(); timestamp = (Get-Date).ToUniversalTime().ToString('o'); correlationId = 'manual-fix-1'; orderId = $order; reason = 'Manually failed after DLT' } | ConvertTo-Json -Compress
"$order|$json" | kubectl -n eda-demo exec -i deploy/kafka -- /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.result --reader-property parse.key=true --reader-property key.separator='|'
```

Overené: objednávka `1c116cc8-…` → `PAYMENT_FAILED`, `failureReason = Manually failed after DLT`, cesta cez `cancel_order`.

Prečo nové `eventId`? Pôvodný `PaymentResult` pre túto objednávku nikdy nevznikol, takže „pôvodné“ ID neexistuje – vymýšľaš novú udalosť. Výhoda: keď ten istý príkaz (s tým istým `$json`) omylom pošleš dvakrát, druhá kópia bude `Duplicate … skipped`.

---

## Cvičenie 7 – Výpadok Zeebe a redrive

**Zadanie:** Vypni Camundu, pošli objednávku, nájdi ju v DLT, obnov Camundu a objednávku dokonči.

**Nápoveda:** `kubectl scale statefulset/camunda --replicas=0`; DLT je `orders.created.DLT`; pri redrive použi **pôvodný** `eventId`. Po obnove počkaj, kým v logu `order-process` prestanú `Failed to activate jobs`.

**Riešenie:** [kapitola 09](09_chybove_scenare.md), kroky 5 a 6. Overené: objednávka `707fa902-…` skončila v DLT po ~3,5 s, prvý redrive krátko po tom, čo bol pod Ready, spadol znova do DLT (gRPC ešte neodpovedal), druhý redrive o ~40 s neskôr → `PAID`. Po obnove znova spusti port-forward na 8088.

Kontrolná otázka: Prečo pôvodné `eventId`? (Redrive je idempotentný – ak by Zeebe správu predsa mal, odmietne ju a druhá inštancia nevznikne.)

---

## Cvičenie 8 – `SKIP LOCKED`

**Zadanie:** Ukáž, že dve transakcie, ktoré chcú zamknúť tie isté riadky outboxu, si ich rozdelia bez čakania.

**Nápoveda:** `Start-Job` na pozadí s `BEGIN` / `FOR UPDATE` / `pg_sleep` / `COMMIT`, druhá session s `FOR UPDATE SKIP LOCKED`. Zamykaj **publikované** riadky, aby si neovplyvnil relay.

**Riešenie:** [kapitola 05](05_outbox_a_inbox.md), krok 7. Overené: druhá session vrátila len riadok `2`.

Rozšírenie: spusti druhú session **bez** `SKIP LOCKED` a zmeraj čas:

```powershell
$job = Start-Job { kubectl -n eda-demo exec statefulset/postgres -- psql -U postgres -d eda -c "BEGIN" -c "SELECT id FROM orders.outbox WHERE id = 1 FOR UPDATE" -c "SELECT pg_sleep(6)" -c "COMMIT" }
Start-Sleep -Seconds 2
$sw = [Diagnostics.Stopwatch]::StartNew()
kubectl -n eda-demo exec statefulset/postgres -- psql -U postgres -d eda -c "SELECT id FROM orders.outbox WHERE id IN (1, 2) ORDER BY id FOR UPDATE;"
"waited {0} ms" -f $sw.ElapsedMilliseconds
Receive-Job $job -Wait | Out-Null; Remove-Job $job
```

```
 id
----
  1
  2
(2 rows)

waited 4233 ms
```

Bez `SKIP LOCKED` druhá session **čakala** ~4 s, kým prvá nespravila `COMMIT`, a potom dostala oba riadky. Dve repliky relaye by sa takto navzájom brzdili – druhá by stála, kým prvá neodošle celú dávku, namiesto toho, aby si vzala inú.

---

## Cvičenie 9 – Timer: zruš objednávku, keď platba nepríde do 10 minút

**Zadanie:** Uprav BPMN tak, aby poison objednávka nečakala navždy: ak `PaymentResult` nepríde do 10 minút, proces pôjde na `Cancel order`.

**Nápoveda:**

- Pozor: **boundary event sa v BPMN dá pripnúť len na aktivitu** (task, subproces), nie na udalosť. `payment_result` je *intermediate catch event*, takže priamo naň timer nepripneš.
- Dve správne možnosti: (a) zmeniť `payment_result` na **receive task** a pripnúť naň timer boundary event, alebo (b) pred čakanie dať **event-based gateway** s dvoma vetvami: správa `PaymentResult` a timer `PT10M`.
- `CancelOrderWorker` posiela `failureReason` – pri timeoute bude `null`. Nastav ho (output mapping na timeri alebo `zeebe:ioMapping`), aby objednávka mala zmysluplný dôvod.

**Riešenie (variant a, neoverené):**

```xml
<bpmn:receiveTask id="payment_result" name="Payment result" messageRef="message_payment_result">
  <bpmn:incoming>flow_to_payment_result</bpmn:incoming>
  <bpmn:outgoing>flow_to_gateway</bpmn:outgoing>
</bpmn:receiveTask>

<bpmn:boundaryEvent id="payment_timeout" name="10 min" attachedToRef="payment_result">
  <bpmn:extensionElements>
    <zeebe:ioMapping>
      <zeebe:output source="=&quot;Payment timeout&quot;" target="failureReason" />
    </zeebe:ioMapping>
  </bpmn:extensionElements>
  <bpmn:outgoing>flow_timeout</bpmn:outgoing>
  <bpmn:timerEventDefinition id="payment_timeout_definition">
    <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">PT10M</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:boundaryEvent>

<bpmn:sequenceFlow id="flow_timeout" sourceRef="payment_timeout" targetRef="cancel_order" />
```

A v `cancel_order` pridať `<bpmn:incoming>flow_timeout</bpmn:incoming>`. Do `BPMNDiagram` treba doplniť tvary (najľahšie: upraviť v Desktop Modeleri, ten ich dokreslí).

Ďalej: `.\scripts\build-images.ps1`, `kubectl -n eda-demo rollout restart deploy/order-process`, overiť novú verziu v `process-definitions/search` (verzia 2). Na skúšku dočasne `PT30S` a poslať sumu 666.

**Na zamyslenie:** čo ak `PaymentCompleted` príde **po** timeoute? Objednávka je zrušená, ale peniaze stiahnuté. Treba kompenzáciu (refund) – to je dôvod, prečo README hovorí „proces nemá timeout ani kompenzace“.

**Neoverené:** zmena BPMN a nasadenie sú mimo rozsahu overovania týchto poznámok. XML je napísané podľa BPMN 2.0 a Zeebe schémy použitej v `order-fulfillment.bpmn`, ale nebolo nasadené.

---

## Cvičenie 10 – Nový krok v procese

**Zadanie:** Po `Confirm order` pridaj krok „Notify customer“, ktorý (zatiaľ) len zaloguje, že zákazníkovi by odišiel e-mail.

**Nápoveda:**

1. BPMN: nový service task medzi `confirm_order` a `order_confirmed`, `zeebe:taskDefinition type="notify-customer" retries="3"`.
2. `ProcessMessages`: konštanta `JOB_NOTIFY_CUSTOMER = "notify-customer"`.
3. Nový `@Component` s metódou `@JobWorker(type = ProcessMessages.JOB_NOTIFY_CUSTOMER)`, čítajúci `OrderVariables`, s `CorrelationScope` kvôli logom.
4. Unit test podľa vzoru `ConfirmOrderWorkerTest` a rozšírenie `OrderProcessIntegrationTest.should_confirmOrder_whenPaymentCompleted` o `"notify_customer"` v `hasCompletedElements`.

**Riešenie (náčrt, neoverené):**

```java
/** Service task „Notify customer“: zatiaľ len zaloguje notifikáciu zákazníka. */
@Component
public class NotifyCustomerWorker {

    private static final Logger log = LoggerFactory.getLogger(NotifyCustomerWorker.class);

    /** Zaloguje notifikáciu; job sa dokončí po návrate. */
    @JobWorker(type = ProcessMessages.JOB_NOTIFY_CUSTOMER)
    public void notifyCustomer(ActivatedJob job) {
        OrderVariables variables = job.getVariablesAsType(OrderVariables.class);
        try (CorrelationScope ignored = CorrelationScope.open(variables.correlationId())) {
            log.info("Customer notified about paid order {}", variables.orderId());
        }
    }
}
```

`CorrelationScope` je package-private v `cz.demo.eda.process.worker` – nový worker teda patrí do toho istého balíčka.

Na zamyslenie: keby worker naozaj posielal e-mail, je idempotentný? Čo sa stane, keď spadne po odoslaní a pred `completeJob`? (Zeebe job zopakuje → druhý e-mail. Riešenie: ID e-mailu odvodené z `jobKey` a deduplikácia v e-mailovej službe – rovnaký vzor ako `commandId`.)

**Neoverené:** zmena kódu a nasadenie sú mimo rozsahu overovania.

---

**Ďalej:** [13 – Zhrnutie](13_zhrnutie.md)
