package cz.demo.eda.process.event;

import java.time.Instant;
import java.util.UUID;

/** Společná metadata všech zpráv; eventId slouží konzumentům k deduplikaci. */
public interface DomainEvent {

    UUID eventId();

    Instant timestamp();

    String correlationId();

    /** Klíč zprávy v Kafce – zaručuje pořadí událostí jedné objednávky v rámci partition. */
    String orderId();
}
