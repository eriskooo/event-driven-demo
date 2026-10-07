package cz.demo.eda.payment.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsTest {

    @Test
    @DisplayName("Vrátí název DLT topicu s příponou .DLT")
    void should_appendDltSuffix_whenTopicGiven() {
        assertThat(Topics.dltOf(Topics.ORDERS_CREATED)).isEqualTo("orders.created.DLT");
    }

    @Test
    @DisplayName("Pro prázdný název vrátí jen příponu")
    void should_returnSuffixOnly_whenTopicIsEmpty() {
        assertThat(Topics.dltOf("")).isEqualTo(".DLT");
    }
}
