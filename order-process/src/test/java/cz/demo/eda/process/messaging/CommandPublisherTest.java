package cz.demo.eda.process.messaging;

import cz.demo.eda.process.support.Tracing;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommandPublisherTest {

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Test
    @DisplayName("Odešle příkaz s klíčem a correlationId v hlavičce a počká na potvrzení brokeru")
    @SuppressWarnings("unchecked")
    void should_sendRecordWithHeader_whenCorrelationIdPresent() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        new CommandPublisher(kafkaTemplate).send("payments.commands", "o-1", "payload", "corr-1");

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<Object, Object> record = captor.getValue();
        assertThat(record.topic()).isEqualTo("payments.commands");
        assertThat(record.key()).isEqualTo("o-1");
        assertThat(record.value()).isEqualTo("payload");
        assertThat(new String(record.headers().lastHeader(Tracing.CORRELATION_ID_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("corr-1");
    }

    @Test
    @DisplayName("Bez correlationId hlavičku nepřidá")
    @SuppressWarnings("unchecked")
    void should_omitHeader_whenCorrelationIdNull() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        new CommandPublisher(kafkaTemplate).send("orders.commands", "o-1", "payload", null);

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().headers().lastHeader(Tracing.CORRELATION_ID_HEADER)).isNull();
    }

    @Test
    @DisplayName("Selhání odeslání převede na CommandPublishException – job se nedokončí a Zeebe ho zopakuje")
    @SuppressWarnings("unchecked")
    void should_throw_whenSendFails() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> new CommandPublisher(kafkaTemplate).send("orders.commands", "o-1", "p", "c"))
                .isInstanceOf(CommandPublishException.class)
                .hasMessageContaining("orders.commands");
    }

    @Test
    @DisplayName("Stejný jobKey dá vždy stejné commandId, různé jobKey různá")
    void should_deriveStableCommandId_whenSameJobKey() {
        assertThat(CommandPublisher.commandId(42L)).isEqualTo(CommandPublisher.commandId(42L));
        assertThat(CommandPublisher.commandId(42L)).isNotEqualTo(CommandPublisher.commandId(43L));
    }

    @Test
    @DisplayName("Pro hraniční jobKey 0 i Long.MAX_VALUE vrátí platné UUID")
    void should_returnUuid_whenJobKeyAtBoundaries() {
        assertThat(CommandPublisher.commandId(0L)).isNotNull();
        assertThat(CommandPublisher.commandId(Long.MAX_VALUE)).isNotEqualTo(CommandPublisher.commandId(0L));
    }
}
