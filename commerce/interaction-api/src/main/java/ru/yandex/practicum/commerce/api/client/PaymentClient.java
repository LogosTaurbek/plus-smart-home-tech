package ru.yandex.practicum.commerce.api.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.api.model.PaymentDto;

@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentClient {

    @PostMapping
    PaymentDto payment(@RequestBody OrderDto orderDto);

    @PostMapping("/totalCost")
    double getTotalCost(@RequestBody OrderDto orderDto);

    @PostMapping("/productCost")
    double getProductCost(@RequestBody OrderDto orderDto);

    @PostMapping("/failed")
    void paymentFailed(@RequestBody OrderDto orderDto);

    @PostMapping("/refund")
    void paymentRefund(@RequestBody OrderDto orderDto);
}
