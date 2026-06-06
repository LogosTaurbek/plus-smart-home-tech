package ru.yandex.practicum.commerce.shoppingcart.mapper;

import ru.yandex.practicum.commerce.api.model.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppingcart.model.Cart;
import ru.yandex.practicum.commerce.shoppingcart.model.CartItem;

import java.util.stream.Collectors;

public class CartMapper {
    public static ShoppingCartDto toDto(Cart cart) {
        return ShoppingCartDto.builder()
                .shoppingCartId(cart.getShoppingCartId())
                .products(cart.getItems().stream()
                        .collect(Collectors.toMap(CartItem::getProductId, CartItem::getQuantity)))
                .build();
    }
}
