package ru.yandex.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.client.DeliveryClient;
import ru.yandex.practicum.commerce.api.model.DeliveryDto;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;

import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class DeliveryController implements DeliveryClient {

    private final DeliveryService deliveryService;

    @Override
    public DeliveryDto planDelivery(OrderDto orderDto) {
        return deliveryService.planDelivery(orderDto);
    }

    @Override
    public double calculateDeliveryCost(OrderDto orderDto) {
        return deliveryService.calculateDeliveryCost(orderDto);
    }

    @Override
    public void pickedByDelivery(UUID deliveryId) {
        deliveryService.pickedByDelivery(deliveryId);
    }

    @Override
    public void deliverySuccessful(UUID deliveryId) {
        deliveryService.deliverySuccessful(deliveryId);
    }

    @Override
    public void deliveryFailed(UUID deliveryId) {
        deliveryService.deliveryFailed(deliveryId);
    }
}
