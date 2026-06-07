package ru.yandex.practicum.commerce.delivery.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.api.model.enums.DeliveryStatus;

import java.util.UUID;

@Entity
@Table(name = "deliveries")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Delivery {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID deliveryId;

    @Column(nullable = false)
    private UUID orderId;

    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "country", column = @Column(name = "from_country")),
        @AttributeOverride(name = "city", column = @Column(name = "from_city")),
        @AttributeOverride(name = "street", column = @Column(name = "from_street")),
        @AttributeOverride(name = "house", column = @Column(name = "from_house")),
        @AttributeOverride(name = "flat", column = @Column(name = "from_flat"))
    })
    private Address fromAddress;

    @Embedded
    @AttributeOverrides({
        @AttributeOverride(name = "country", column = @Column(name = "to_country")),
        @AttributeOverride(name = "city", column = @Column(name = "to_city")),
        @AttributeOverride(name = "street", column = @Column(name = "to_street")),
        @AttributeOverride(name = "house", column = @Column(name = "to_house")),
        @AttributeOverride(name = "flat", column = @Column(name = "to_flat"))
    })
    private Address toAddress;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status;

    @Column(nullable = false)
    private double weight;

    @Column(nullable = false)
    private double volume;

    @Column(nullable = false)
    private boolean fragile;
}
