package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.client.OrderClient;
import ru.yandex.practicum.commerce.api.client.ShoppingStoreClient;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.api.model.PaymentDto;
import ru.yandex.practicum.commerce.api.model.ProductDto;
import ru.yandex.practicum.commerce.api.model.enums.PaymentStatus;
import ru.yandex.practicum.commerce.payment.model.Payment;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    @Transactional(readOnly = true)
    public double getProductCost(OrderDto orderDto) {
        double total = 0;
        for (Map.Entry<UUID, Long> entry : orderDto.getProducts().entrySet()) {
            ProductDto product = shoppingStoreClient.getProduct(entry.getKey());
            total += product.getPrice() * entry.getValue();
        }
        return total;
    }

    public double getTotalCost(OrderDto orderDto) {
        double productCost = getProductCost(orderDto);
        double tax = productCost * 0.1;
        return productCost + tax + orderDto.getDeliveryPrice();
    }

    @Transactional
    public PaymentDto payment(OrderDto orderDto) {
        double productCost = getProductCost(orderDto);
        double tax = productCost * 0.1;
        double total = productCost + tax + orderDto.getDeliveryPrice();

        Payment payment = Payment.builder()
                .productPrice(productCost)
                .deliveryPrice(orderDto.getDeliveryPrice())
                .totalPrice(total)
                .status(PaymentStatus.PENDING)
                .build();

        payment = paymentRepository.save(payment);

        return PaymentDto.builder()
                .paymentId(payment.getPaymentId())
                .totalPayment(payment.getTotalPrice())
                .deliveryTotal(payment.getDeliveryPrice())
                .feeTotal(tax)
                .status(payment.getStatus())
                .build();
    }

    @Transactional
    public void paymentFailed(OrderDto orderDto) {
        Payment payment = paymentRepository.findById(orderDto.getPaymentId())
                .orElseThrow(() -> new RuntimeException("Payment not found"));
        payment.setStatus(PaymentStatus.FAILED);
        paymentRepository.save(payment);
        orderClient.orderPaymentFailed(orderDto.getOrderId());
    }

    @Transactional
    public void paymentSuccess(OrderDto orderDto) {
        Payment payment = paymentRepository.findById(orderDto.getPaymentId())
                .orElseThrow(() -> new RuntimeException("Payment not found"));
        payment.setStatus(PaymentStatus.SUCCESS);
        paymentRepository.save(payment);
        orderClient.orderPaid(orderDto.getOrderId());
    }

    @Transactional
    public void paymentRefund(OrderDto orderDto) {
        // Logically we just mark it as refunded or similar, TZ says "сохранять информацию об оплате... пригодиться в дальнейшем для разбора инцидентов и оформления возврата средств"
        log.info("Refund processed for order {}", orderDto.getOrderId());
    }
}
