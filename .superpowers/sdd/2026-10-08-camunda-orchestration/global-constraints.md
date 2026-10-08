## Global Constraints

- **Necommitovat a nepushovat** – git operace dělá uživatel. Místo commitu každý task končí zelenými testy (`mvn -q test` v dotčené službě).
- Žádný sdílený kód mezi službami – každá služba má vlastní kopii tříd událostí a podpůrných tříd; kontraktem je JSON.
- Java 21, žádné `var`, explicitní typy; records, sealed interfaces, pattern matching `switch`.
- `@Transactional` jen na servisách/repozitářích (order-process transakce nemá vůbec).
- Komentáře a JavaDoc česky (každá public metoda má JavaDoc), komentovat PROČ; identifikátory anglicky; konstanty `UPPER_CASE`.
- Testy: JUnit 5, `should_doSomething_whenCondition()`, `@DisplayName` česky, Mockito pro závislosti; každá nová public metoda má test.
- Logování přes SLF4J; žádné `System.out`; nikdy prázdný `catch`.
- Verze: Camunda **8.10.2** (Maven i Docker image), Kafka `apache/kafka:4.3.1`, PostgreSQL `postgres:18.6-alpine`.
- Topicy: `orders.created`, `payments.commands`, `payments.result`, `orders.commands` (3 partitions, klíč = `orderId`), DLT = `<topic>.DLT`.
- Spring type hlavičky vypnuté (`spring.json.add.type.headers: false`), correlationId v těle i hlavičce `X-Correlation-Id`.
- Upřesnění oproti specu (stejné chování, jednodušší kód): (1) topicy zakládá každá služba pro topicy, které produkuje nebo konzumuje, plus DLT svých konzumentů – stejná konvence jako dosud; (2) job workery používají `autoComplete = true` a odesílají do Kafky synchronně – job se tedy dokončí až po ack brokeru, výjimka job nedokončí.

