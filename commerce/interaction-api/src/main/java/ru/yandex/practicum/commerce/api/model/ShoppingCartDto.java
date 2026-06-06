package ru.yandex.practicum.commerce.api.model;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCartDto {
    @NotNull
    private UUID shoppingCartId;
    @NotNull
    private Map<UUID, Long> products;
}
