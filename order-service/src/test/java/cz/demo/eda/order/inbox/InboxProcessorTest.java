package cz.demo.eda.order.inbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InboxProcessorTest {

    @Mock
    private InboxService inbox;

    private InboxProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new InboxProcessor(inbox);
    }

    @Test
    @DisplayName("Zpracuje všechny připravené zprávy v pořadí")
    void should_processEachDueMessage_whenRun() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        when(inbox.findDueIds()).thenReturn(List.of(first, second));

        processor.processDue();

        verify(inbox).process(first);
        verify(inbox).process(second);
        verify(inbox, never()).recordFailure(any(), any());
    }

    @Test
    @DisplayName("Neúspěšné zpracování zaznamená v nové transakci a pokračuje další zprávou")
    void should_recordFailureAndContinue_whenProcessingFails() {
        UUID failing = UUID.randomUUID();
        UUID next = UUID.randomUUID();
        IllegalStateException error = new IllegalStateException("boom");
        when(inbox.findDueIds()).thenReturn(List.of(failing, next));
        when(inbox.process(failing)).thenThrow(error);

        processor.processDue();

        verify(inbox).recordFailure(failing, error);
        verify(inbox).process(next);
    }

    @Test
    @DisplayName("Bez připravených zpráv nic nezpracuje")
    void should_doNothing_whenNothingDue() {
        when(inbox.findDueIds()).thenReturn(List.of());

        processor.processDue();

        verify(inbox, never()).process(any());
    }

    @Test
    @DisplayName("Chyba infrastruktury běh ukončí bez vyhození výjimky")
    void should_swallowAndLog_whenInboxUnavailable() {
        when(inbox.findDueIds()).thenThrow(new IllegalStateException("db down"));

        processor.processDue();

        verify(inbox, never()).process(any());
    }
}
