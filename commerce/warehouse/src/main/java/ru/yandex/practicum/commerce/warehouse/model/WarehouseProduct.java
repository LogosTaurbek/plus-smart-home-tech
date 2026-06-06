package ru.yandex.practicum.commerce.warehouse.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@Table(name = "warehouse_products")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseProduct {
    @Id
    private UUID productId;

    @Column(nullable = false)
    private long quantity;

    @Column(nullable = false)
    private double weight;

    @Column(nullable = false)
    private boolean fragile;

    @Embedded
    private Dimension dimension;
}
