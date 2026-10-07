package cz.demo.eda.payment.domain;

import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.event.PaymentCompleted;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.outbox.OutboxPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private PaymentSimulator simulator;
    @Mock
    private PaymentRepository repository;
    @Mock
    private OutboxPublisher outbox;

    private PaymentService service;

    @BeforeEach
    void setUp() {
        service = new PaymentService(simulator, repository, outbox, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Úspěšnou platbu uloží a výsledek zařadí do outboxu payments.result")
    void should_saveAndPublish_whenPaymentCompleted() {
        OrderCreated order = order("10.00");
        PaymentCompleted result = PaymentCompleted.of("c", "o-1", "p-1", order.amount());
        when(simulator.process(order)).thenReturn(result);

        assertThat(service.processPayment(order)).isEqualTo(result);

        Payment saved = captureSaved();
        assertThat(saved.status()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(saved.createdAt()).isEqualTo(NOW);
        verify(outbox).publish("payments.result", result);
    }

    @Test
    @DisplayName("Zamítnutou platbu také uloží a publikuje PaymentFailed")
    void should_saveAndPublish_whenPaymentDeclined() {
        OrderCreated order = order("10.00");
        PaymentFailed result = PaymentFailed.of("c", "o-1", "declined");
        when(simulator.process(order)).thenReturn(result);

        service.processPayment(order);

        assertThat(captureSaved().status()).isEqualTo(PaymentStatus.FAILED);
        verify(outbox).publish("payments.result", result);
    }

    @Test
    @DisplayName("Technická chyba simulace nic neuloží ani nepublikuje")
    void should_propagate_whenSimulatorFails() {
        OrderCreated order = order("666");
        when(simulator.process(order)).thenThrow(new PaymentProcessingException("boom"));

        assertThatThrownBy(() -> service.processPayment(order)).isInstanceOf(PaymentProcessingException.class);

        verifyNoInteractions(repository, outbox);
    }

    private Payment captureSaved() {
        ArgumentCaptor<Payment> captor = ArgumentCaptor.forClass(Payment.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    private static OrderCreated order(String amount) {
        return OrderCreated.of("c", "o-1", "cust", new BigDecimal(amount), "CZK");
    }
}
