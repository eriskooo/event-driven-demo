package cz.demo.eda.payment;

import cz.demo.eda.payment.domain.Payment;
import cz.demo.eda.payment.domain.PaymentRepository;
import cz.demo.eda.payment.domain.PaymentSimulator;
import cz.demo.eda.payment.domain.PaymentStatus;
import cz.demo.eda.payment.event.ProcessPayment;
import cz.demo.eda.payment.inbox.InboxEntry;
import cz.demo.eda.payment.inbox.InboxRepository;
import cz.demo.eda.payment.inbox.InboxStatus;
import cz.demo.eda.payment.outbox.OutboxPublisher;
import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
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
        "eda.inbox.max-attempts=4",
        "eda.inbox.initial-backoff=50ms",
        "eda.inbox.max-backoff=200ms",
        "eda.inbox.poll-interval-ms=100",
        "eda.outbox.poll-interval-ms=100"})
@Import({KafkaTestcontainer.class, PostgresTestcontainer.class})
class PaymentServiceIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
    @Autowired
    private JsonMapper jsonMapper;
    @Autowired
    private MeterRegistry registry;
    @Autowired
    private InboxRepository inbox;
    @Autowired
    private PaymentRepository payments;
    @MockitoSpyBean
    private PaymentSimulator simulator;

    @Test
    @DisplayName("ProcessPayment přes inbox a outbox vede k PaymentCompleted se stejným orderId a correlationId")
    void should_publishPaymentCompleted_whenProcessPayment() throws Exception {
        ProcessPayment order = ProcessPayment.of("it-corr-2", "order-ok", new BigDecimal("15.00"), "CZK");

        send(order);

        ConsumerRecord<String, String> result = KafkaTestSupport.awaitRecord(kafka.getBootstrapServers(),
                Topics.PAYMENTS_RESULT, r -> "order-ok".equals(r.key()), TIMEOUT);
        JsonNode json = jsonMapper.readTree(result.value());
        assertThat(json.get("type").asString()).isEqualTo("PaymentCompleted");
        assertThat(json.get("correlationId").asString()).isEqualTo("it-corr-2");
        assertThat(KafkaTestSupport.header(result, Tracing.CORRELATION_ID_HEADER)).isEqualTo("it-corr-2");
        assertThat(payments.findByOrderId("order-ok")).get().extracting(Payment::status).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(inboxStatus(order)).isEqualTo(InboxStatus.PROCESSED);
    }

    @Test
    @DisplayName("Duplicitní ProcessPayment vytvoří jen jeden výsledek platby")
    void should_publishSingleResult_whenProcessPaymentDeliveredTwice() throws Exception {
        ProcessPayment order = ProcessPayment.of("it-corr-3", "order-dup", BigDecimal.TEN, "CZK");

        send(order);
        send(order);

        await().atMost(TIMEOUT).until(() -> counter(MessagingMetrics.CONSUMED, "outcome", "duplicate") >= 1);
        List<ConsumerRecord<String, String>> results = KafkaTestSupport.collectRecords(kafka.getBootstrapServers(),
                Topics.PAYMENTS_RESULT, r -> "order-dup".equals(r.key()), Duration.ofSeconds(3));
        assertThat(results).hasSize(1);
    }

    @Test
    @DisplayName("Poison objednávka se v inboxu zkusí 4krát s backoffem, skončí v DLT a offset je hned potvrzený")
    void should_retryInInboxThenDeadLetter_whenAmountIsPoison() throws Exception {
        ProcessPayment order = ProcessPayment.of("it-corr-4", "order-poison", new BigDecimal("666"), "CZK");

        send(order);

        ConsumerRecord<String, String> dead = KafkaTestSupport.awaitRecord(kafka.getBootstrapServers(),
                Topics.dltOf(Topics.PAYMENTS_COMMANDS), r -> "order-poison".equals(r.key()), TIMEOUT);
        assertThat(jsonMapper.readTree(dead.value()).get("orderId").asString()).isEqualTo("order-poison");
        assertThat(KafkaTestSupport.header(dead, OutboxPublisher.DLT_EXCEPTION_FQCN)).endsWith("PaymentProcessingException");
        assertThat(KafkaTestSupport.header(dead, OutboxPublisher.DLT_ATTEMPTS)).isEqualTo("4");
        assertThat(KafkaTestSupport.header(dead, Tracing.CORRELATION_ID_HEADER)).isEqualTo("it-corr-4");
        verify(simulator, times(4)).process(argThat(o -> "order-poison".equals(o.orderId())));
        assertThat(inboxStatus(order)).isEqualTo(InboxStatus.FAILED);
        // Rollback neúspěšných pokusů nesmí nechat v DB platbu.
        assertThat(payments.findByOrderId("order-poison")).isEmpty();
        assertThat(counter(MessagingMetrics.DEAD_LETTERED, "topic", Topics.dltOf(Topics.PAYMENTS_COMMANDS))).isEqualTo(1.0);
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumerLag("payment-service")).isZero());
        assertThat(KafkaTestSupport.collectRecords(kafka.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-poison".equals(r.key()), Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    @DisplayName("Zpráva v orders.created už platbu nespustí – platbu řídí jen příkaz z payments.commands")
    void should_ignoreOrdersCreated_whenOrchestrated() {
        KafkaTestSupport.sendRaw(kafka.getBootstrapServers(), "orders.created", "order-legacy",
                "{\"eventId\":\"6f1c2d3e-0000-4000-8000-000000000001\",\"timestamp\":\"2026-10-08T10:00:00Z\","
                        + "\"orderId\":\"order-legacy\",\"customerId\":\"c\",\"amount\":10,\"currency\":\"CZK\"}");

        assertThat(KafkaTestSupport.collectRecords(kafka.getBootstrapServers(), Topics.PAYMENTS_RESULT,
                r -> "order-legacy".equals(r.key()), Duration.ofSeconds(3))).isEmpty();
    }

    private InboxStatus inboxStatus(ProcessPayment order) {
        return inbox.findById(order.eventId()).map(InboxEntry::status).orElseThrow();
    }

    private void send(ProcessPayment order) throws Exception {
        ProducerRecord<Object, Object> record = new ProducerRecord<>(Topics.PAYMENTS_COMMANDS, order.orderId(), order);
        record.headers().add(Tracing.CORRELATION_ID_HEADER, order.correlationId().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get();
    }

    private double counter(String name, String tagKey, String tagValue) {
        Counter counter = registry.find(name).tag(tagKey, tagValue).counter();
        return counter == null ? 0 : counter.count();
    }

    /** Součet rozdílů mezi koncem partitions a commitnutým offsetem consumer group. */
    private long consumerLag(String groupId) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
            Map<TopicPartition, OffsetSpec> partitions = admin.describeTopics(List.of(Topics.PAYMENTS_COMMANDS))
                    .allTopicNames().get().get(Topics.PAYMENTS_COMMANDS).partitions().stream()
                    .map(p -> new TopicPartition(Topics.PAYMENTS_COMMANDS, p.partition()))
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(partitions).all().get();
            // Partition bez commitu vrací Admin API jako klíč s hodnotou null.
            return ends.entrySet().stream()
                    .mapToLong(e -> {
                        OffsetAndMetadata offset = committed.get(e.getKey());
                        return e.getValue().offset() - (offset == null ? 0 : offset.offset());
                    })
                    .sum();
        }
    }
}
