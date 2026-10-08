package cz.demo.eda.process.process;

import cz.demo.eda.process.config.ProcessProperties;
import io.camunda.client.CamundaClient;
import io.camunda.client.api.command.ClientStatusException;
import io.camunda.client.api.command.PublishMessageCommandStep1.PublishMessageCommandStep3;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// LENIENT: společný stub řetězu v setUp test s null messageId nevyužije.
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProcessGatewayTest {

    private static final UUID MESSAGE_ID = UUID.fromString("5b7c0000-0000-4000-8000-000000000001");
    private static final Map<String, Object> VARIABLES = Map.of("orderId", "o-1");

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CamundaClient client;

    private PublishMessageCommandStep3 command;
    private ProcessGateway gateway;

    @BeforeEach
    void setUp() {
        gateway = new ProcessGateway(client, new ProcessProperties(Duration.ofMinutes(1)));
        command = client.newPublishMessageCommand().messageName("PaymentResult").correlationKey("o-1");
        when(command.messageId(MESSAGE_ID.toString()).timeToLive(Duration.ofMinutes(1)).variables(VARIABLES))
                .thenReturn(command);
        // stubování výše volá messageId() samo; verify má počítat jen volání z produkčního kódu
        clearInvocations(command);
    }

    @Test
    @DisplayName("Publikuje zprávu s názvem, correlation key, messageId, TTL a proměnnými")
    void should_publishMessage_whenCalled() {
        boolean published = gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES);

        assertThat(published).isTrue();
        verify(command).messageId(MESSAGE_ID.toString());
        verify(command).send();
    }

    @Test
    @DisplayName("Vrátí false když Zeebe zprávu se stejným messageId už má (duplicitní doručení z Kafky)")
    void should_returnFalse_whenMessageAlreadyExists() {
        when(command.send().join()).thenThrow(new ClientStatusException(Status.ALREADY_EXISTS, null));

        assertThat(gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES)).isFalse();
    }

    @Test
    @DisplayName("Jinou chybu klienta propaguje, aby Kafka error handler zprávu zopakoval")
    void should_rethrow_whenOtherClientError() {
        when(command.send().join()).thenThrow(new ClientStatusException(Status.UNAVAILABLE, null));

        assertThatThrownBy(() -> gateway.publish("PaymentResult", "o-1", MESSAGE_ID, VARIABLES))
                .isInstanceOf(ClientStatusException.class);
    }

    @Test
    @DisplayName("Odmítne null messageId – bez něj by Zeebe neuměl deduplikovat")
    void should_throw_whenMessageIdIsNull() {
        assertThatThrownBy(() -> gateway.publish("PaymentResult", "o-1", null, VARIABLES))
                .isInstanceOf(NullPointerException.class);
    }
}
