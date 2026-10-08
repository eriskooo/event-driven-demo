package cz.demo.eda.process.process;

/** Názvy z BPMN modelu order-fulfillment – musí odpovídat souboru bpmn/order-fulfillment.bpmn. */
public final class ProcessMessages {

    public static final String PROCESS_ID = "order-fulfillment";
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String PAYMENT_RESULT = "PaymentResult";
    public static final String JOB_REQUEST_PAYMENT = "request-payment";
    public static final String JOB_CONFIRM_ORDER = "confirm-order";
    public static final String JOB_CANCEL_ORDER = "cancel-order";

    private ProcessMessages() {
    }
}
