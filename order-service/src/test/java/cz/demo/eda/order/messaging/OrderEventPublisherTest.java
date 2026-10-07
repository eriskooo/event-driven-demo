package cz.demo.eda.order.messaging;

import cz.demo.eda.order.support.Topics;
import cz.demo.eda.order.support.Tracing;
import cz.demo.eda.order.event.OrderCreated;
import cz.demo.eda.order.support.MessagingMetrics;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private SimpleMeterRegistry registry;
    private OrderEventPublisher publisher;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        publisher = new OrderEventPublisher(kafkaTemplate, new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Odešle záznam s klíčem orderId a hlavičkou correlationId a započítá ho")
    void should_sendKeyedRecordWithHeader_whenPublished() {
        var event = OrderCreated.of("corr-1", "o-1", "c-1", BigDecimal.TEN, "CZK");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(inv -> success(inv.getArgument(0)));

        publisher.publish(event).join();

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<?, ?> record = captor.getValue();
        assertThat(record.topic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.value()).isEqualTo(event);
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-1");
        assertThat(registry.get(MessagingMetrics.PRODUCED).counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Bez correlationId odešle záznam bez hlavičky")
    void should_omitHeader_whenCorrelationIdIsNull() {
        var event = OrderCreated.of(null, "o-1", "c-1", BigDecimal.TEN, "CZK");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(inv -> success(inv.getArgument(0)));

        publisher.publish(event).join();

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().headers().lastHeader(Tracing.CORRELATION_ID_HEADER)).isNull();
    }

    @Test
    @DisplayName("Při selhání odeslání nezapočítá zprávu jako odeslanou")
    void should_notCountProduced_whenSendFails() {
        var event = OrderCreated.of("c", "o-1", "c-1", BigDecimal.TEN, "CZK");
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        var future = publisher.publish(event);

        assertThat(future).isCompletedExceptionally();
        assertThat(registry.find(MessagingMetrics.PRODUCED).counter()).isNull();
    }

    private static CompletableFuture<SendResult<String, Object>> success(ProducerRecord<String, Object> record) {
        var metadata = new RecordMetadata(new TopicPartition(record.topic(), 0), 0, 0, 0, 0, 0);
        return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
    }
}
