package ru.yandex.practicum.commerce.order.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.client.OrderClient;
import ru.yandex.practicum.commerce.api.model.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class OrderController implements OrderClient {

    private final OrderService orderService;

    @Override
    public List<OrderDto> getOrders(String username) {
        // Simple implementation, normally repository query
        return List.of();
    }

    @Override
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        return orderService.createNewOrder(request);
    }

    @Override
    public OrderDto calculateTotalCost(UUID orderId) {
        return orderService.calculateTotalCost(orderId);
    }

    @Override
    public OrderDto calculateDeliveryCost(UUID orderId) {
        return orderService.calculateDeliveryCost(orderId);
    }

    @Override
    public OrderDto orderAssembled(UUID orderId) {
        return orderService.orderAssembled(orderId);
    }

    @Override
    public OrderDto orderAssemblyFailed(UUID orderId) {
        return orderService.orderAssemblyFailed(orderId);
    }

    @Override
    public OrderDto orderPaid(UUID orderId) {
        return orderService.orderPaid(orderId);
    }

    @Override
    public OrderDto orderPaymentFailed(UUID orderId) {
        return orderService.orderPaymentFailed(orderId);
    }

    @Override
    public OrderDto orderDelivered(UUID orderId) {
        return orderService.orderDelivered(orderId);
    }

    @Override
    public OrderDto orderDeliveryFailed(UUID orderId) {
        return orderService.orderDeliveryFailed(orderId);
    }

    @Override
    public OrderDto orderCompleted(UUID orderId) {
        return orderService.orderCompleted(orderId);
    }

    @Override
    public OrderDto orderReturned(UUID orderId) {
        return orderService.orderReturned(orderId);
    }
}
