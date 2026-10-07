package cz.demo.eda.payment.support;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Paměť zpracovaných eventId pro idempotentního konzumenta.
 * Kapacita je omezená (LRU), protože Kafka doručuje "at least once" a duplicity
 * přichází typicky krátce po originálu – neomezená množina by jen rostla.
 */
public class ProcessedEventStore {

    public static final int DEFAULT_CAPACITY = 10_000;

    private final Set<UUID> processed;

    /** Vytvoří úložiště s výchozí kapacitou. */
    public ProcessedEventStore() {
        this(DEFAULT_CAPACITY);
    }

    /** Vytvoří úložiště, které si pamatuje nejvýše {@code capacity} posledních eventId. */
    public ProcessedEventStore(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        processed = Collections.synchronizedSet(Collections.newSetFromMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                return size() > capacity;
            }
        }));
    }

    /** Vrátí true, pokud už byla událost se zadaným eventId zpracována. */
    public boolean isProcessed(UUID eventId) {
        return processed.contains(Objects.requireNonNull(eventId, "eventId"));
    }

    /** Označí událost jako zpracovanou; volat až po úspěšném zpracování, aby retry nebyl přeskočen. */
    public void markProcessed(UUID eventId) {
        processed.add(Objects.requireNonNull(eventId, "eventId"));
    }

    /** Vrátí počet pamatovaných eventId. */
    public int size() {
        return processed.size();
    }
}
