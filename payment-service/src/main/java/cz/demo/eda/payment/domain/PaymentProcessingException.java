package cz.demo.eda.payment.domain;

/** Technická chyba zpracování platby – na rozdíl od zamítnutí ji řeší retry a DLT. */
public class PaymentProcessingException extends RuntimeException {

    public PaymentProcessingException(String message) {
        super(message);
    }
}
