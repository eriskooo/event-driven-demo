package cz.demo.eda.order.api;

import cz.demo.eda.order.support.Tracing;
import cz.demo.eda.order.domain.OrderService;
import jakarta.validation.Valid;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/** REST API pro zakládání a dotazování objednávek. */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** Založí objednávku; platba proběhne asynchronně, proto vrací stav PENDING_PAYMENT. */
    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        var order = orderService.createOrder(request.customerId(), request.amount(), request.currency(),
                MDC.get(Tracing.CORRELATION_ID_MDC_KEY));
        var location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(order.id()).toUri();
        return ResponseEntity.created(location).body(OrderResponse.from(order));
    }

    /** Vrátí objednávku podle ID nebo 404. */
    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable String id) {
        return ResponseEntity.of(orderService.findById(id).map(OrderResponse::from));
    }
}
