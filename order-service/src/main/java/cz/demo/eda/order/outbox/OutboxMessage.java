package cz.demo.eda.order.outbox;

import java.util.Map;
import java.util.UUID;

/**
 * Řádek outboxu čekající na publikaci.
 *
 * @param payload událost serializovaná do JSON už při zápisu – relay ji posílá beze změny
 * @param headers Kafka hlavičky zprávy (correlationId, u DLT i důvod selhání)
 */
public record OutboxMessage(long id, UUID eventId, String topic, String key, String payload,
                            Map<String, String> headers) {

    public OutboxMessage {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
