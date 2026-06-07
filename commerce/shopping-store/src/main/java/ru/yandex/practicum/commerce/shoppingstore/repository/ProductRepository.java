package ru.yandex.practicum.commerce.shoppingstore.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.api.model.enums.ProductCategory;
import ru.yandex.practicum.commerce.shoppingstore.model.Product;

import java.util.UUID;

public interface ProductRepository extends JpaRepository<Product, UUID> {
    Page<Product> findAllByProductCategory(ProductCategory productCategory, Pageable pageable);
}
