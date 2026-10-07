package cz.demo.eda.payment.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxService outbox;

    private OutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new OutboxRelay(outbox);
    }

    @Test
    @DisplayName("Plnou dávku zopakuje, dokud outbox nevyprázdní")
    void should_repeatBatches_whenBatchIsFull() {
        when(outbox.publishBatch()).thenReturn(OutboxService.BATCH_SIZE, OutboxService.BATCH_SIZE, 3);

        relay.publishPending();

        verify(outbox, times(3)).publishBatch();
    }

    @Test
    @DisplayName("Prázdný outbox zpracuje jedním voláním")
    void should_stop_whenNothingPending() {
        when(outbox.publishBatch()).thenReturn(0);

        relay.publishPending();

        verify(outbox, times(1)).publishBatch();
    }

    @Test
    @DisplayName("Selhání dávky zaloguje a nevyhodí výjimku – zkusí se při dalším běhu")
    void should_swallowAndLog_whenBatchFails() {
        when(outbox.publishBatch()).thenThrow(new IllegalStateException("broker down"));

        relay.publishPending();

        verify(outbox, times(1)).publishBatch();
    }
}
