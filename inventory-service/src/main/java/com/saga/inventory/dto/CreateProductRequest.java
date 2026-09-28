package com.saga.inventory.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateProductRequest(

        @NotBlank(message = "name is required")
        String name,

        @NotNull @Min(value = 0, message = "quantity must be zero or positive")
        Integer quantity
) {}
