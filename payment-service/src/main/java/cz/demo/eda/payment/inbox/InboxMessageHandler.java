package cz.demo.eda.payment.inbox;

/** Zpracuje zprávu z inboxu; běží v transakci processoru, výjimka znamená neúspěšný pokus. */
@FunctionalInterface
public interface InboxMessageHandler {

    /** Zpracuje zprávu (doménová změna + případné odchozí zprávy do outboxu). */
    void handle(InboxEntry entry);
}
