package cz.demo.eda.order.domain;

/** Životní cyklus objednávky řízený výsledkem platby. */
public enum OrderStatus {
    PENDING_PAYMENT,
    PAID,
    PAYMENT_FAILED;

    /** Vrátí true, pokud stav už nelze změnit dalším výsledkem platby. */
    public boolean isFinal() {
        return this != PENDING_PAYMENT;
    }
}
