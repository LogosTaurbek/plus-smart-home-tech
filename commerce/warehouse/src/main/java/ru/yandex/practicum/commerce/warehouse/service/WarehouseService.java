package ru.yandex.practicum.commerce.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.model.*;
import ru.yandex.practicum.commerce.warehouse.model.Dimension;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseService {
    private final WarehouseProductRepository warehouseProductRepository;

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS = ADDRESSES[new SecureRandom().nextInt(ADDRESSES.length)];

    @Transactional
    public void newProductInWarehouse(NewProductInWarehouseRequest request) {
        if (warehouseProductRepository.existsById(request.getProductId())) {
            throw new RuntimeException("Product already registered in warehouse");
        }
        WarehouseProduct product = WarehouseProduct.builder()
                .productId(request.getProductId())
                .weight(request.getWeight())
                .fragile(request.isFragile())
                .quantity(0)
                .dimension(Dimension.builder()
                        .width(request.getDimension().getWidth())
                        .height(request.getDimension().getHeight())
                        .depth(request.getDimension().getDepth())
                        .build())
                .build();
        warehouseProductRepository.save(product);
    }

    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        WarehouseProduct product = warehouseProductRepository.findById(request.getProductId())
                .orElseThrow(() -> new RuntimeException("Product not found in warehouse"));
        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseProductRepository.save(product);
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto shoppingCartDto) {
        log.info("Checking product quantity enough for shopping cart: {}", shoppingCartDto.getShoppingCartId());
        double totalWeight = 0;
        double totalVolume = 0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Long> entry : shoppingCartDto.getProducts().entrySet()) {
            WarehouseProduct product = warehouseProductRepository.findById(entry.getKey())
                    .orElseThrow(() -> new RuntimeException("Product " + entry.getKey() + " not found in warehouse"));

            if (product.getQuantity() < entry.getValue()) {
                throw new RuntimeException("Insufficient quantity for product: " + entry.getKey());
            }

            totalWeight += product.getWeight() * entry.getValue();
            totalVolume += (product.getDimension().getWidth() *
                            product.getDimension().getHeight() *
                            product.getDimension().getDepth()) * entry.getValue();
            if (product.isFragile()) {
                hasFragile = true;
            }
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }

    public AddressDto getWarehouseAddress() {
        return AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
    }
}
