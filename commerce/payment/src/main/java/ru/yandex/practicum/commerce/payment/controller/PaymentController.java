package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.client.PaymentClient;
import ru.yandex.practicum.commerce.api.model.OrderDto;
import ru.yandex.practicum.commerce.api.model.PaymentDto;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

@RestController
@RequiredArgsConstructor
public class PaymentController implements PaymentClient {

    private final PaymentService paymentService;

    @Override
    public PaymentDto payment(OrderDto orderDto) {
        return paymentService.payment(orderDto);
    }

    @Override
    public double getTotalCost(OrderDto orderDto) {
        return paymentService.getTotalCost(orderDto);
    }

    @Override
    public double getProductCost(OrderDto orderDto) {
        return paymentService.getProductCost(orderDto);
    }

    @Override
    public void paymentFailed(OrderDto orderDto) {
        paymentService.paymentFailed(orderDto);
    }

    @Override
    public void paymentRefund(OrderDto orderDto) {
        paymentService.paymentRefund(orderDto);
    }
}
