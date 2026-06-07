package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "order_bookings")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderBooking {
    @Id
    private UUID orderId;

    private UUID deliveryId;

    @ElementCollection
    @CollectionTable(name = "order_booking_products", joinColumns = @JoinColumn(name = "order_id"))
    @MapKeyColumn(name = "product_id")
    @Column(name = "quantity")
    private Map<UUID, Long> products;
}
