package ru.yandex.practicum.commerce.api.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.api.model.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.api.model.OrderDto;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderClient {

    @GetMapping
    List<OrderDto> getOrders(@RequestParam String username);

    @PutMapping
    OrderDto createNewOrder(@RequestBody CreateNewOrderRequest request);

    @PostMapping("/calculate/total")
    OrderDto calculateTotalCost(@RequestBody UUID orderId);

    @PostMapping("/calculate/delivery")
    OrderDto calculateDeliveryCost(@RequestBody UUID orderId);

    @PostMapping("/assembly")
    OrderDto orderAssembled(@RequestBody UUID orderId);

    @PostMapping("/assembly/failed")
    OrderDto orderAssemblyFailed(@RequestBody UUID orderId);

    @PostMapping("/payment")
    OrderDto orderPaid(@RequestBody UUID orderId);

    @PostMapping("/payment/failed")
    OrderDto orderPaymentFailed(@RequestBody UUID orderId);

    @PostMapping("/delivery")
    OrderDto orderDelivered(@RequestBody UUID orderId);

    @PostMapping("/delivery/failed")
    OrderDto orderDeliveryFailed(@RequestBody UUID orderId);

    @PostMapping("/completed")
    OrderDto orderCompleted(@RequestBody UUID orderId);

    @PostMapping("/return")
    OrderDto orderReturned(@RequestBody UUID orderId);
}
