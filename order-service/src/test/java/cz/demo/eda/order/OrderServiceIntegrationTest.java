package cz.demo.eda.order;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import cz.demo.eda.order.event.PaymentCompleted;
import cz.demo.eda.order.domain.OrderRepository;
import cz.demo.eda.order.domain.OrderStatus;
import cz.demo.eda.order.support.MessagingMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "eda.kafka.retry.initial-interval=50ms",
        "eda.kafka.retry.max-interval=100ms"})
@AutoConfigureMockMvc
@Testcontainers
class OrderServiceIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Autowired
    private MockMvc mvc;
    @Autowired
    private JsonMapper jsonMapper;
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private OrderRepository repository;
    @Autowired
    private MeterRegistry registry;

    @Test
    @DisplayName("Objednávka se publikuje do orders.created a PaymentCompleted ji převede na PAID právě jednou")
    void should_completeOrder_whenPaymentCompletedReceived() throws Exception {
        var body = mvc.perform(post("/orders")
                        .header(Tracing.CORRELATION_ID_HEADER, "it-corr-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"cust-it","amount":42.00,"currency":"CZK"}"""))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        var orderId = jsonMapper.readTree(body).get("id").asString();

        var published = KafkaTestSupport.awaitRecord(KAFKA.getBootstrapServers(), Topics.ORDERS_CREATED,
                r -> orderId.equals(r.key()), TIMEOUT);
        var event = jsonMapper.readTree(published.value());
        assertThat(event.get("orderId").asString()).isEqualTo(orderId);
        assertThat(event.get("correlationId").asString()).isEqualTo("it-corr-1");
        assertThat(event.hasNonNull("eventId")).isTrue();
        assertThat(KafkaTestSupport.header(published, Tracing.CORRELATION_ID_HEADER)).isEqualTo("it-corr-1");

        var payment = PaymentCompleted.of("it-corr-1", orderId, "pay-1", new BigDecimal("42.00"));
        sendPaymentResult(payment);
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(repository.findById(orderId)).get().extracting(o -> o.status()).isEqualTo(OrderStatus.PAID));

        // Stejná událost podruhé (at-least-once doručení) – musí být rozpoznána jako duplicita.
        sendPaymentResult(payment);
        await().atMost(TIMEOUT).until(() -> counter(MessagingMetrics.CONSUMED, "outcome", "duplicate") >= 1);
        assertThat(repository.findById(orderId)).get().extracting(o -> o.paymentId()).isEqualTo("pay-1");
    }

    @Test
    @DisplayName("Nečitelná zpráva v payments.result skončí v DLT bez retry a započítá se")
    void should_moveToDlt_whenPaymentResultUnreadable() {
        KafkaTestSupport.sendRaw(KAFKA.getBootstrapServers(), Topics.PAYMENTS_RESULT, "bad-1", "this is not json");

        var dead = KafkaTestSupport.awaitRecord(KAFKA.getBootstrapServers(), Topics.dltOf(Topics.PAYMENTS_RESULT),
                r -> "bad-1".equals(r.key()), TIMEOUT);

        assertThat(dead.value()).isEqualTo("this is not json");
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotBlank();
        await().atMost(TIMEOUT).until(() ->
                counter(MessagingMetrics.DEAD_LETTERED, "topic", Topics.dltOf(Topics.PAYMENTS_RESULT)) == 1.0);
    }

    private void sendPaymentResult(PaymentCompleted payment) throws Exception {
        var record = new ProducerRecord<Object, Object>(Topics.PAYMENTS_RESULT, payment.orderId(), payment);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, payment.correlationId().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }

    private double counter(String name, String tagKey, String tagValue) {
        var counter = registry.find(name).tag(tagKey, tagValue).counter();
        return counter == null ? 0 : counter.count();
    }
}
