package ru.yandex.practicum.commerce.payment.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.api.model.enums.PaymentStatus;

import java.util.UUID;

@Entity
@Table(name = "payments")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID paymentId;

    @Column(nullable = false)
    private double productPrice;

    @Column(nullable = false)
    private double deliveryPrice;

    @Column(nullable = false)
    private double totalPrice;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;
}
