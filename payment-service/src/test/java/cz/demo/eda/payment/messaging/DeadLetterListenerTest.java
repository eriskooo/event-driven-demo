package cz.demo.eda.payment.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DeadLetterListenerTest {

    private final DeadLetterListener listener = new DeadLetterListener();

    @Test
    @DisplayName("Zprávu z DLT zaloguje a potvrdí")
    void should_acknowledge_whenDeadLetterReceived() {
        var record = new ConsumerRecord<String, String>("orders.created.DLT", 0, 0, "o-1", "{}");
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, "boom".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(7).array());
        var ack = mock(Acknowledgment.class);

        listener.onDeadLetter(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Zprávu bez DLT hlaviček a s null hodnotou zpracuje bez chyby")
    void should_acknowledge_whenHeadersAndValueMissing() {
        var record = new ConsumerRecord<String, String>("orders.created.DLT", 0, 0, null, null);
        var ack = mock(Acknowledgment.class);

        listener.onDeadLetter(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Vrátí text hlavičky, nebo null když chybí")
    void should_readHeaderAsText_whenPresent() {
        var record = new ConsumerRecord<String, String>("t", 0, 0, "k", "v");
        record.headers().add("h", "value".getBytes(StandardCharsets.UTF_8));

        assertThat(DeadLetterListener.header(record.headers(), "h")).isEqualTo("value");
        assertThat(DeadLetterListener.header(record.headers(), "missing")).isNull();
    }
}
