package cz.demo.eda.payment.messaging;

import cz.demo.eda.payment.support.Topics;
import cz.demo.eda.payment.support.Tracing;
import cz.demo.eda.payment.event.PaymentFailed;
import cz.demo.eda.payment.support.MessagingMetrics;
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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentResultPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private SimpleMeterRegistry registry;
    private PaymentResultPublisher publisher;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        publisher = new PaymentResultPublisher(kafkaTemplate, new MessagingMetrics(registry));
    }

    @Test
    @DisplayName("Odešle výsledek s klíčem orderId a hlavičkou correlationId")
    void should_sendKeyedRecord_whenPublished() {
        var result = PaymentFailed.of("corr-7", "o-7", "declined");
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, Object> record = inv.getArgument(0);
            var metadata = new RecordMetadata(new TopicPartition(record.topic(), 1), 0, 5, 0, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(record, metadata));
        });

        publisher.publish(result);

        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<?, ?> record = captor.getValue();
        assertThat(record.topic()).isEqualTo(Topics.PAYMENTS_RESULT);
        assertThat(record.key()).isEqualTo("o-7");
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-7");
        assertThat(registry.get(MessagingMetrics.PRODUCED).counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Selhání odeslání propaguje výjimku, aby se offset nepotvrdil")
    void should_throw_whenSendFails() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> publisher.publish(PaymentFailed.of("c", "o", "r")))
                .isInstanceOf(CompletionException.class);
        assertThat(registry.find(MessagingMetrics.PRODUCED).counter()).isNull();
    }
}
