package ru.yandex.practicum.commerce.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "ru.yandex.practicum.commerce.api.client")
public class DeliveryApp {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryApp.class, args);
    }
}
