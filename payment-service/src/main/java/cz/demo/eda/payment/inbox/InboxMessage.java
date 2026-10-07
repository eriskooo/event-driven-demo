package cz.demo.eda.payment.inbox;

import java.util.UUID;

/**
 * Přijatá zpráva čekající na zpracování.
 *
 * @param attempts počet dosud neúspěšných pokusů o zpracování
 */
public record InboxMessage(UUID eventId, String topic, String key, String payload, String correlationId,
                           int attempts) {
}
