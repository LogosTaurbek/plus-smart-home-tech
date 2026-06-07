package ru.yandex.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.client.OrderClient;
import ru.yandex.practicum.commerce.api.client.WarehouseClient;
import ru.yandex.practicum.commerce.api.model.AddressDto;
import ru.yandex.practicum.commerce.api.model.DeliveryDto;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.api.model.ShippedToDeliveryRequest;
import ru.yandex.practicum.commerce.api.model.enums.DeliveryStatus;
import ru.yandex.practicum.commerce.delivery.model.Address;
import ru.yandex.practicum.commerce.delivery.model.Delivery;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final OrderClient orderClient;
    private final WarehouseClient warehouseClient;

    @Transactional
    public DeliveryDto planDelivery(OrderDto orderDto) {
        AddressDto warehouseAddrDto = warehouseClient.getWarehouseAddress();
        
        Delivery delivery = Delivery.builder()
                .orderId(orderDto.getOrderId())
                .fromAddress(mapAddress(warehouseAddrDto))
                .toAddress(mapAddress(orderDto.getDeliveryAddress())) // Wait, OrderDto needs deliveryAddress field? Let's check model.
                .status(DeliveryStatus.CREATED)
                .weight(orderDto.getDeliveryWeight())
                .volume(orderDto.getDeliveryVolume())
                .fragile(orderDto.isFragile())
                .build();

        delivery = deliveryRepository.save(delivery);

        return mapToDto(delivery);
    }

    public double calculateDeliveryCost(OrderDto orderDto) {
        double cost = 5.0;
        AddressDto warehouseAddr = warehouseClient.getWarehouseAddress();
        
        double multiplier = 1.0;
        if (warehouseAddr.getStreet().contains("ADDRESS_2")) {
            multiplier = 2.0;
        }
        
        cost = cost * multiplier + 5.0;

        if (orderDto.isFragile()) {
            cost = cost * 0.2 + cost;
        }

        cost += orderDto.getDeliveryWeight() * 0.3;
        cost += orderDto.getDeliveryVolume() * 0.2;

        if (!orderDto.getDeliveryAddress().getStreet().equals(warehouseAddr.getStreet())) {
            cost = cost * 0.2 + cost;
        }

        return cost;
    }

    @Transactional
    public void pickedByDelivery(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setStatus(DeliveryStatus.IN_PROGRESS);
        deliveryRepository.save(delivery);

        orderClient.orderAssembled(delivery.getOrderId()); // TZ says "ASSEMBLED"
        
        warehouseClient.shippedToDelivery(ShippedToDeliveryRequest.builder()
                .orderId(delivery.getOrderId())
                .deliveryId(deliveryId)
                .build());
    }

    @Transactional
    public void deliverySuccessful(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setStatus(DeliveryStatus.DELIVERED);
        deliveryRepository.save(delivery);

        orderClient.orderDelivered(delivery.getOrderId());
    }

    @Transactional
    public void deliveryFailed(UUID deliveryId) {
        Delivery delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new RuntimeException("Delivery not found"));
        delivery.setStatus(DeliveryStatus.FAILED);
        deliveryRepository.save(delivery);

        orderClient.orderDeliveryFailed(delivery.getOrderId());
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

    private DeliveryDto mapToDto(Delivery delivery) {
        return DeliveryDto.builder()
                .deliveryId(delivery.getDeliveryId())
                .fromAddress(mapToAddressDto(delivery.getFromAddress()))
                .toAddress(mapToAddressDto(delivery.getToAddress()))
                .orderId(delivery.getOrderId())
                .deliveryState(delivery.getStatus())
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
}
