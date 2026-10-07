package cz.demo.eda.payment;

import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import cz.demo.eda.payment.event.OrderCreated;
import cz.demo.eda.payment.domain.PaymentSimulator;
import cz.demo.eda.payment.support.MessagingMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "payment.failure-rate=0",
        "eda.kafka.retry.max-retries=3",
        "eda.kafka.retry.initial-interval=50ms",
        "eda.kafka.retry.max-interval=100ms"})
@Testcontainers
class PaymentServiceIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Container
    @ServiceConnection
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private JsonMapper jsonMapper;
    @Autowired
    private MeterRegistry registry;
    @MockitoSpyBean
    private PaymentSimulator simulator;

    @Test
    @DisplayName("OrderCreated vede k PaymentCompleted v payments.result se stejným orderId a correlationId")
    void should_publishPaymentCompleted_whenOrderCreated() throws Exception {
        var order = OrderCreated.of("it-corr-2", "order-ok", "cust", new BigDecimal("15.00"), "CZK");

        send(order);

        var result = KafkaTestSupport.awaitRecord(KAFKA.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-ok".equals(r.key()), TIMEOUT);
        var json = jsonMapper.readTree(result.value());
        assertThat(json.get("type").asString()).isEqualTo("PaymentCompleted");
        assertThat(json.get("correlationId").asString()).isEqualTo("it-corr-2");
        assertThat(KafkaTestSupport.header(result, Tracing.CORRELATION_ID_HEADER)).isEqualTo("it-corr-2");
    }

    @Test
    @DisplayName("Duplicitní OrderCreated vytvoří jen jeden výsledek platby")
    void should_publishSingleResult_whenOrderCreatedDeliveredTwice() throws Exception {
        var order = OrderCreated.of("it-corr-3", "order-dup", "cust", BigDecimal.TEN, "CZK");

        send(order);
        send(order);

        await().atMost(TIMEOUT).until(() -> counter(MessagingMetrics.CONSUMED, "outcome", "duplicate") >= 1);
        var results = KafkaTestSupport.collectRecords(KAFKA.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-dup".equals(r.key()), Duration.ofSeconds(3));
        assertThat(results).hasSize(1);
    }

    @Test
    @DisplayName("Poison objednávka se zkusí 1+3krát, skončí v DLT a její offset se commitne")
    void should_retryThenDeadLetter_whenAmountIsPoison() throws Exception {
        var order = OrderCreated.of("it-corr-4", "order-poison", "cust", new BigDecimal("666"), "CZK");

        send(order);

        var dead = KafkaTestSupport.awaitRecord(KAFKA.getBootstrapServers(), Topics.dltOf(Topics.ORDERS_CREATED),
                r -> "order-poison".equals(r.key()), TIMEOUT);
        assertThat(jsonMapper.readTree(dead.value()).get("orderId").asString()).isEqualTo("order-poison");
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
                + KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN)).contains("PaymentProcessingException");
        verify(simulator, times(4)).process(argThat(o -> "order-poison".equals(o.orderId())));
        await().atMost(TIMEOUT).until(() ->
                counter(MessagingMetrics.DEAD_LETTERED, "topic", Topics.dltOf(Topics.ORDERS_CREATED)) == 1.0);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumerLag("payment-service")).isZero());
        assertThat(KafkaTestSupport.collectRecords(KAFKA.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-poison".equals(r.key()), Duration.ofSeconds(2))).isEmpty();
    }

    private void send(OrderCreated order) throws Exception {
        var record = new ProducerRecord<Object, Object>(Topics.ORDERS_CREATED, order.orderId(), order);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, order.correlationId().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }

    private double counter(String name, String tagKey, String tagValue) {
        var counter = registry.find(name).tag(tagKey, tagValue).counter();
        return counter == null ? 0 : counter.count();
    }

    /** Součet rozdílů mezi koncem partitions a commitnutým offsetem consumer group. */
    private long consumerLag(String groupId) throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            var committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            var partitions = admin.describeTopics(List.of(Topics.ORDERS_CREATED)).allTopicNames().get()
                    .get(Topics.ORDERS_CREATED).partitions().stream()
                    .map(p -> new TopicPartition(Topics.ORDERS_CREATED, p.partition()))
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
            var ends = admin.listOffsets(partitions).all().get();
            // Partition bez commitu vrací Admin API jako klíč s hodnotou null.
            return ends.entrySet().stream()
                    .mapToLong(e -> {
                        var offset = committed.get(e.getKey());
                        return e.getValue().offset() - (offset == null ? 0 : offset.offset());
                    })
                    .sum();
        }
    }
}
