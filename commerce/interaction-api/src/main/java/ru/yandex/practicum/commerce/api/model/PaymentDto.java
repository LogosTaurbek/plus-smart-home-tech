package ru.yandex.practicum.commerce.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.api.model.enums.PaymentStatus;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentDto {
    private UUID paymentId;
    private double totalPayment;
    private double deliveryTotal;
    private double feeTotal;
    private PaymentStatus status;
}
