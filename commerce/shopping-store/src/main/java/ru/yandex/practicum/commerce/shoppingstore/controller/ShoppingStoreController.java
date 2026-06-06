package ru.yandex.practicum.commerce.shoppingstore.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.api.client.ShoppingStoreClient;
import ru.yandex.practicum.commerce.api.model.ProductDto;
import ru.yandex.practicum.commerce.api.model.enums.ProductCategory;
import ru.yandex.practicum.commerce.api.model.enums.QuantityState;
import ru.yandex.practicum.commerce.shoppingstore.service.ShoppingStoreService;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
public class ShoppingStoreController implements ShoppingStoreClient {
    private final ShoppingStoreService shoppingStoreService;

    @Override
    public Page<ProductDto> getProducts(ProductCategory category, int page, int size, List<String> sort) {
        List<Sort.Order> orders = sort.stream()
                .map(s -> {
                    String[] parts = s.split(",");
                    return parts.length > 1 && parts[1].equalsIgnoreCase("desc")
                            ? Sort.Order.desc(parts[0])
                            : Sort.Order.asc(parts[0]);
                })
                .collect(Collectors.toList());

        return shoppingStoreService.getProducts(category, PageRequest.of(page, size, Sort.by(orders)));
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        return shoppingStoreService.getProduct(productId);
    }

    @Override
    public ProductDto createNewProduct(ProductDto productDto) {
        return shoppingStoreService.createNewProduct(productDto);
    }

    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        return shoppingStoreService.updateProduct(productDto);
    }

    @Override
    public boolean removeProductFromStore(UUID productId) {
        return shoppingStoreService.removeProductFromStore(productId);
    }

    @Override
    public boolean setProductQuantityState(UUID productId, QuantityState quantityState) {
        return shoppingStoreService.setProductQuantityState(productId, quantityState);
    }
}
