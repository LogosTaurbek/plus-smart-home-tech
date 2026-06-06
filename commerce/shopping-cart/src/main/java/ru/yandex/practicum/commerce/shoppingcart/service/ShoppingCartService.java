package ru.yandex.practicum.commerce.shoppingcart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.api.client.WarehouseClient;
import ru.yandex.practicum.commerce.api.model.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.mapper.CartMapper;
import ru.yandex.practicum.commerce.shoppingcart.model.Cart;
import ru.yandex.practicum.commerce.shoppingcart.model.CartItem;
import ru.yandex.practicum.commerce.shoppingcart.repository.CartRepository;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartService {
    private final CartRepository cartRepository;
    private final WarehouseClient warehouseClient;

    @Transactional
    public ShoppingCartDto getShoppingCart(String username) {
        log.info("Retrieving shopping cart for user: {}", username);
        Cart cart = cartRepository.findByUsernameAndActive(username, true)
                .orElseGet(() -> cartRepository.save(Cart.builder()
                        .username(username)
                        .active(true)
                        .items(new ArrayList<>())
                        .build()));
        return CartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductToShoppingCart(String username, Map<UUID, Long> products) {
        Cart cart = cartRepository.findByUsernameAndActive(username, true)
                .orElseGet(() -> Cart.builder()
                        .username(username)
                        .active(true)
                        .items(new ArrayList<>())
                        .build());

        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            long quantity = entry.getValue();

            cart.getItems().stream()
                    .filter(item -> item.getProductId().equals(productId))
                    .findFirst()
                    .ifPresentOrElse(
                            item -> item.setQuantity(item.getQuantity() + quantity),
                            () -> cart.getItems().add(CartItem.builder()
                                    .cart(cart)
                                    .productId(productId)
                                    .quantity(quantity)
                                    .build())
                    );
        }

        // Validate with warehouse
        try {
            warehouseClient.checkProductQuantityEnoughForShoppingCart(CartMapper.toDto(cart));
        } catch (Exception e) {
            log.error("Warehouse check failed: {}", e.getMessage());
            throw new RuntimeException("Insufficient stock in warehouse", e);
        }

        return CartMapper.toDto(cartRepository.save(cart));
    }

    @Transactional
    public void deactivateCurrentShoppingCart(String username) {
        cartRepository.findByUsernameAndActive(username, true)
                .ifPresent(cart -> {
                    cart.setActive(false);
                    cartRepository.save(cart);
                });
    }
}
