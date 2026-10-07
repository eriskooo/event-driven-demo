package cz.demo.eda.payment.event;

import java.time.Instant;
import java.util.UUID;

/** Společná metadata všech událostí; eventId slouží konzumentům k deduplikaci. */
public interface DomainEvent {

    UUID eventId();

    Instant timestamp();

    String correlationId();

    /** Klíč zprávy v Kafce – zaručuje pořadí událostí jedné objednávky v rámci partition. */
    String orderId();
}
