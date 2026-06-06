package ru.yandex.practicum.commerce.api.model;

import jakarta.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimensionDto {
    @Min(1)
    private double width;
    @Min(1)
    private double height;
    @Min(1)
    private double depth;
}
