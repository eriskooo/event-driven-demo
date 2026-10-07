package cz.demo.eda.order.api;

import cz.demo.eda.order.support.Tracing;
import cz.demo.eda.order.domain.Order;
import cz.demo.eda.order.domain.OrderService;
import cz.demo.eda.order.support.CorrelationIdFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
@Import(CorrelationIdFilter.class)
class OrderControllerTest {

    private static final Order ORDER = Order.create("o-1", "c-1", new BigDecimal("10.00"), "CZK", Instant.EPOCH);

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private OrderService orderService;

    @Test
    @DisplayName("POST /orders vrátí 201 s Location a předá correlationId z hlavičky")
    void should_createOrder_whenRequestValid() throws Exception {
        when(orderService.createOrder(anyString(), any(), anyString(), anyString())).thenReturn(ORDER);

        mvc.perform(post("/orders")
                        .header(Tracing.CORRELATION_ID_HEADER, "corr-123")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"c-1","amount":10.00,"currency":"CZK"}"""))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "http://localhost/orders/o-1"))
                .andExpect(header().string(Tracing.CORRELATION_ID_HEADER, "corr-123"))
                .andExpect(jsonPath("$.id").value("o-1"))
                .andExpect(jsonPath("$.status").value("PENDING_PAYMENT"));

        verify(orderService).createOrder(eq("c-1"), any(BigDecimal.class), eq("CZK"), eq("corr-123"));
    }

    @Test
    @DisplayName("POST /orders bez correlationId vygeneruje nové a vrátí ho v hlavičce")
    void should_generateCorrelationId_whenHeaderMissing() throws Exception {
        when(orderService.createOrder(anyString(), any(), anyString(), anyString())).thenReturn(ORDER);

        mvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"c-1","amount":10.00,"currency":"CZK"}"""))
                .andExpect(status().isCreated())
                .andExpect(header().exists(Tracing.CORRELATION_ID_HEADER));
    }

    @Test
    @DisplayName("POST /orders s nulovou částkou vrátí 400")
    void should_returnBadRequest_whenAmountIsZero() throws Exception {
        mvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"c-1","amount":0,"currency":"CZK"}"""))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(orderService);
    }

    @Test
    @DisplayName("POST /orders s chybějícími poli vrátí 400")
    void should_returnBadRequest_whenBodyIsEmpty() throws Exception {
        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /orders s neplatnou měnou vrátí 400")
    void should_returnBadRequest_whenCurrencyInvalid() throws Exception {
        mvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"c-1","amount":1,"currency":"czk"}"""))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("GET /orders/{id} vrátí existující objednávku")
    void should_returnOrder_whenExists() throws Exception {
        when(orderService.findById("o-1")).thenReturn(Optional.of(ORDER));

        mvc.perform(get("/orders/o-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.customerId").value("c-1"))
                .andExpect(jsonPath("$.amount").value(10.00));
    }

    @Test
    @DisplayName("GET /orders/{id} vrátí 404 pro neznámou objednávku")
    void should_returnNotFound_whenOrderUnknown() throws Exception {
        when(orderService.findById("x")).thenReturn(Optional.empty());

        mvc.perform(get("/orders/x")).andExpect(status().isNotFound());
    }
}
