package ru.yandex.practicum.commerce.api.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.api.model.DeliveryDto;
import ru.yandex.practicum.commerce.api.model.OrderDto;

import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryClient {

    @PutMapping
    DeliveryDto planDelivery(@RequestBody OrderDto orderDto);

    @PostMapping("/cost")
    double calculateDeliveryCost(@RequestBody OrderDto orderDto);

    @PostMapping("/picked")
    void pickedByDelivery(@RequestBody UUID deliveryId);

    @PostMapping("/successful")
    void deliverySuccessful(@RequestBody UUID deliveryId);

    @PostMapping("/failed")
    void deliveryFailed(@RequestBody UUID deliveryId);
}
