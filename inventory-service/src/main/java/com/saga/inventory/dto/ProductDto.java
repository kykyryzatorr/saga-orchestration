package com.saga.inventory.dto;

import com.saga.inventory.entity.Product;

import java.time.LocalDateTime;
import java.util.UUID;

public record ProductDto(
        UUID id,
        String name,
        Integer quantity,
        Integer reservedQuantity,
        Integer availableQuantity,
        LocalDateTime createdAt
) {
    public static ProductDto from(Product product) {
        return new ProductDto(
                product.getId(),
                product.getName(),
                product.getQuantity(),
                product.getReservedQuantity(),
                product.getAvailableQuantity(),
                product.getCreatedAt()
        );
    }
}
