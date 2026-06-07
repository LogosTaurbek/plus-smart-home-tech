package ru.yandex.practicum.commerce.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.api.model.enums.OrderState;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {
    private UUID orderId;
    private UUID shoppingCartId;
    private Map<UUID, Long> products;
    private UUID paymentId;
    private UUID deliveryId;
    private AddressDto deliveryAddress;
    private OrderState state;
    private double deliveryWeight;
    private double deliveryVolume;
    private boolean fragile;
    private double totalPrice;
    private double productPrice;
    private double deliveryPrice;
}
