package ru.yandex.practicum.commerce.order.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.api.model.enums.OrderState;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID orderId;

    private UUID shoppingCartId;

    private UUID paymentId;

    private UUID deliveryId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderState state;

    private double deliveryWeight;

    private double deliveryVolume;

    private boolean fragile;

    private double totalPrice;

    private double productPrice;

    private double deliveryPrice;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    @Builder.Default
    private List<OrderItem> items = new ArrayList<>();

    @Embedded
    private Address deliveryAddress;
}
