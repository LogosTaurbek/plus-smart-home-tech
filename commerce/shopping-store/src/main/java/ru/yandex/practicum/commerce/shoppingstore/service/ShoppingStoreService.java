package ru.yandex.practicum.commerce.shoppingstore.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import ru.yandex.practicum.commerce.api.model.ProductDto;
import ru.yandex.practicum.commerce.api.model.enums.ProductCategory;
import ru.yandex.practicum.commerce.api.model.enums.ProductState;
import ru.yandex.practicum.commerce.api.model.enums.QuantityState;
import ru.yandex.practicum.commerce.shoppingstore.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shoppingstore.model.Product;
import ru.yandex.practicum.commerce.shoppingstore.repository.ProductRepository;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ShoppingStoreService {
    private final ProductRepository productRepository;

    @Transactional(readOnly = true)
    public Page<ProductDto> getProducts(ProductCategory category, Pageable pageable) {
        return productRepository.findAllByProductCategory(category, pageable)
                .map(ProductMapper::toDto);
    }

    @Transactional(readOnly = true)
    public ProductDto getProduct(UUID productId) {
        return productRepository.findById(productId)
                .map(ProductMapper::toDto)
                .orElseThrow(() -> new RuntimeException("Product not found"));
    }

    @Transactional
    public ProductDto createNewProduct(ProductDto productDto) {
        Product product = ProductMapper.toEntity(productDto);
        return ProductMapper.toDto(productRepository.save(product));
    }

    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        if (!productRepository.existsById(productDto.getProductId())) {
            throw new RuntimeException("Product not found");
        }
        Product product = ProductMapper.toEntity(productDto);
        return ProductMapper.toDto(productRepository.save(product));
    }

    @Transactional
    public boolean removeProductFromStore(UUID productId) {
        return productRepository.findById(productId)
                .map(product -> {
                    product.setProductState(ProductState.DEACTIVATE);
                    productRepository.save(product);
                    return true;
                })
                .orElse(false);
    }

    @Transactional
    public boolean setProductQuantityState(UUID productId, QuantityState quantityState) {
        return productRepository.findById(productId)
                .map(product -> {
                    product.setQuantityState(quantityState);
                    productRepository.save(product);
                    return true;
                })
                .orElse(false);
    }
}
