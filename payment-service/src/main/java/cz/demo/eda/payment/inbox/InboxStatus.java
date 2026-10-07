package cz.demo.eda.payment.inbox;

/** Stav zprávy v inboxu. */
public enum InboxStatus {
    /** Čeká na (další) pokus o zpracování. */
    PENDING,
    /** Úspěšně zpracována. */
    PROCESSED,
    /** Vyčerpány pokusy – zpráva odešla do DLT. */
    FAILED
}
