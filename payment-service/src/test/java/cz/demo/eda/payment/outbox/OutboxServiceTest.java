package cz.demo.eda.payment.outbox;

import cz.demo.eda.payment.support.MessagingMetrics;
import cz.demo.eda.payment.support.Tracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");
    private static final Limit BATCH = Limit.of(OutboxService.BATCH_SIZE);

    @Mock
    private OutboxRepository repository;
    @Mock
    private KafkaOperations<String, String> kafka;

    private SimpleMeterRegistry registry;
    private OutboxService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new OutboxService(repository, kafka, new MessagingMetrics(registry), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Odešle zprávu s klíčem, payloadem a hlavičkami a označí ji jako publikovanou")
    void should_sendAndMarkPublished_whenMessagePending() {
        OutboxEntry entry = OutboxEntry.pending(UUID.randomUUID(), "orders.created", "o-1", "{\"a\":1}",
                Map.of(Tracing.CORRELATION_ID_HEADER, "corr-1"), NOW.minusSeconds(1));
        when(repository.lockUnpublished(BATCH)).thenReturn(List.of(entry));
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> success(inv.getArgument(0)));

        assertThat(service.publishBatch()).isEqualTo(1);

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captor.capture());
        ProducerRecord<?, ?> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("orders.created");
        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.value()).isEqualTo("{\"a\":1}");
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-1");
        assertThat(entry.publishedAt()).isEqualTo(NOW);
        assertThat(registry.get(MessagingMetrics.PRODUCED).tag("topic", "orders.created").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Prázdný outbox nic neodesílá")
    void should_doNothing_whenOutboxEmpty() {
        when(repository.lockUnpublished(BATCH)).thenReturn(List.of());

        assertThat(service.publishBatch()).isZero();

        verifyNoInteractions(kafka);
    }

    @Test
    @DisplayName("Při selhání odeslání zprávu neoznačí – zkusí se znovu")
    void should_notMarkPublished_whenSendFails() {
        OutboxEntry entry = OutboxEntry.pending(UUID.randomUUID(), "t", "k", "{}", Map.of(), NOW);
        when(repository.lockUnpublished(BATCH)).thenReturn(List.of(entry));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> service.publishBatch()).isInstanceOf(OutboxService.OutboxPublishException.class);

        assertThat(entry.publishedAt()).isNull();
        assertThat(registry.find(MessagingMetrics.PRODUCED).counter()).isNull();
    }

    private static CompletableFuture<SendResult<String, String>> success(ProducerRecord<String, String> record) {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
    }
}
