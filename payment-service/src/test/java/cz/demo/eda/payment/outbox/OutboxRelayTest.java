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
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionOperations;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxRepository repository;
    @Mock
    private KafkaOperations<String, String> kafka;

    private SimpleMeterRegistry registry;
    private OutboxRelay relay;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        relay = new OutboxRelay(repository, kafka, TransactionOperations.withoutTransaction(), new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Odešle zprávu s klíčem, payloadem a hlavičkami a označí ji jako publikovanou")
    void should_sendAndMarkPublished_whenMessagePending() {
        var message = new OutboxMessage(7, UUID.randomUUID(), "orders.created", "o-1", "{\"a\":1}",
                Map.of(Tracing.CORRELATION_ID_HEADER, "corr-1"));
        when(repository.lockUnpublished(OutboxRelay.BATCH_SIZE)).thenReturn(List.of(message));
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> success(inv.getArgument(0)));

        assertThat(relay.publishBatch()).isEqualTo(1);

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captor.capture());
        ProducerRecord<?, ?> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("orders.created");
        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.value()).isEqualTo("{\"a\":1}");
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-1");
        verify(repository).markPublished(List.of(7L));
        assertThat(registry.get(MessagingMetrics.PRODUCED).tag("topic", "orders.created").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Prázdný outbox nic neodesílá")
    void should_doNothing_whenOutboxEmpty() {
        when(repository.lockUnpublished(OutboxRelay.BATCH_SIZE)).thenReturn(List.of());

        assertThat(relay.publishBatch()).isZero();

        verifyNoInteractions(kafka);
        verify(repository, never()).markPublished(anyList());
    }

    @Test
    @DisplayName("Při selhání odeslání zprávy neoznačí – zkusí se znovu")
    void should_notMarkPublished_whenSendFails() {
        var message = new OutboxMessage(1, UUID.randomUUID(), "t", "k", "{}", Map.of());
        when(repository.lockUnpublished(OutboxRelay.BATCH_SIZE)).thenReturn(List.of(message));
        when(kafka.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        relay.publishPending();

        verify(repository, never()).markPublished(anyList());
        assertThat(registry.find(MessagingMetrics.PRODUCED).counter()).isNull();
    }

    @Test
    @DisplayName("Plnou dávku zopakuje, dokud outbox nevyprázdní")
    void should_continue_whenBatchIsFull() {
        var fullBatch = LongStream.range(0, OutboxRelay.BATCH_SIZE)
                .mapToObj(i -> new OutboxMessage(i, UUID.randomUUID(), "t", "k", "{}", Map.of()))
                .toList();
        when(repository.lockUnpublished(anyInt())).thenReturn(fullBatch, List.of());
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(inv -> success(inv.getArgument(0)));

        relay.publishPending();

        verify(repository, times(2)).lockUnpublished(OutboxRelay.BATCH_SIZE);
    }

    private static CompletableFuture<SendResult<String, String>> success(ProducerRecord<String, String> record) {
        var metadata = new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
    }
}
