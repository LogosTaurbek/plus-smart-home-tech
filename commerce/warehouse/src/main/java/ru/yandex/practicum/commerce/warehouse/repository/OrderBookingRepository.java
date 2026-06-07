package ru.yandex.practicum.commerce.warehouse.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.warehouse.model.OrderBooking;

import java.util.UUID;

public interface OrderBookingRepository extends JpaRepository<OrderBooking, UUID> {
}
