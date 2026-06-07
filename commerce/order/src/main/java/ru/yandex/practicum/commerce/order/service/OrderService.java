package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.client.DeliveryClient;
import ru.yandex.practicum.commerce.api.client.PaymentClient;
import ru.yandex.practicum.commerce.api.client.WarehouseClient;
import ru.yandex.practicum.commerce.api.model.*;
import ru.yandex.practicum.commerce.api.model.enums.OrderState;
import ru.yandex.practicum.commerce.order.model.Address;
import ru.yandex.practicum.commerce.order.model.Order;
import ru.yandex.practicum.commerce.order.model.OrderItem;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final WarehouseClient warehouseClient;
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        Order order = Order.builder()
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .deliveryAddress(mapAddress(request.getDeliveryAddress()))
                .state(OrderState.NEW)
                .items(new ArrayList<>())
                .build();

        for (var entry : request.getShoppingCart().getProducts().entrySet()) {
            order.getItems().add(OrderItem.builder()
                    .order(order)
                    .productId(entry.getKey())
                    .quantity(entry.getValue())
                    .build());
        }

        order = orderRepository.save(order);

        // Assembly at warehouse
        try {
            BookedProductsDto booked = warehouseClient.assemblyProductsForOrder(AssemblyProductsForOrderRequest.builder()
                    .orderId(order.getOrderId())
                    .products(request.getShoppingCart().getProducts())
                    .build());
            
            order.setDeliveryWeight(booked.getDeliveryWeight());
            order.setDeliveryVolume(booked.getDeliveryVolume());
            order.setFragile(booked.isFragile());
            orderRepository.save(order);
        } catch (Exception e) {
            log.error("Assembly failed for order {}", order.getOrderId());
            order.setState(OrderState.ASSEMBLY_FAILED);
            orderRepository.save(order);
            throw new RuntimeException("Assembly failed", e);
        }

        return mapToDto(order);
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        double cost = deliveryClient.calculateDeliveryCost(mapToDto(order));
        order.setDeliveryPrice(cost);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        double total = paymentClient.getTotalCost(mapToDto(order));
        order.setTotalPrice(total);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderAssembled(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.ASSEMBLED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderAssemblyFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.ASSEMBLY_FAILED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderPaid(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.PAID);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderPaymentFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.PAYMENT_FAILED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderDelivered(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.DELIVERED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderDeliveryFailed(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.DELIVERY_FAILED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderCompleted(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.COMPLETED);
        return mapToDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto orderReturned(UUID orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow();
        order.setState(OrderState.PRODUCT_RETURNED);
        
        // Return products to warehouse
        warehouseClient.acceptReturn(order.getItems().stream()
                .collect(Collectors.toMap(OrderItem::getProductId, OrderItem::getQuantity)));
                
        return mapToDto(orderRepository.save(order));
    }

    private Address mapAddress(AddressDto dto) {
        return Address.builder()
                .country(dto.getCountry())
                .city(dto.getCity())
                .street(dto.getStreet())
                .house(dto.getHouse())
                .flat(dto.getFlat())
                .build();
    }

    private AddressDto mapToAddressDto(Address address) {
        return AddressDto.builder()
                .country(address.getCountry())
                .city(address.getCity())
                .street(address.getStreet())
                .house(address.getHouse())
                .flat(address.getFlat())
                .build();
    }

    private OrderDto mapToDto(Order order) {
        return OrderDto.builder()
                .orderId(order.getOrderId())
                .shoppingCartId(order.getShoppingCartId())
                .paymentId(order.getPaymentId())
                .deliveryId(order.getDeliveryId())
                .state(order.getState())
                .deliveryWeight(order.getDeliveryWeight())
                .deliveryVolume(order.getDeliveryVolume())
                .fragile(order.isFragile())
                .totalPrice(order.getTotalPrice())
                .productPrice(order.getProductPrice())
                .deliveryPrice(order.getDeliveryPrice())
                .deliveryAddress(mapToAddressDto(order.getDeliveryAddress()))
                .products(order.getItems().stream()
                        .collect(Collectors.toMap(OrderItem::getProductId, OrderItem::getQuantity)))
                .build();
    }
}
