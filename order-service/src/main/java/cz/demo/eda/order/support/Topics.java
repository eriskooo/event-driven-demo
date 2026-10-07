package cz.demo.eda.order.support;

/** Názvy Kafka topiců – součást kontraktu s ostatními službami, musí odpovídat jejich konfiguraci. */
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String PAYMENTS_RESULT = "payments.result";
    /** Suffix dead letter topiců; resolver v konzumentech jej používá explicitně. */
    public static final String DLT_SUFFIX = ".DLT";

    private Topics() {
    }

    /** Vrátí název dead letter topicu pro zadaný topic. */
    public static String dltOf(String topic) {
        return topic + DLT_SUFFIX;
    }
}
