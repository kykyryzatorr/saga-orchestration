package com.saga.orchestrator.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record StartOrderRequest(

        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "productId is required")
        String productId,

        @NotNull @Min(value = 1, message = "quantity must be at least 1")
        Integer quantity,

        @NotNull @DecimalMin(value = "0.01", message = "unitPrice must be positive")
        BigDecimal unitPrice,

        @NotBlank(message = "currency is required")
        String currency
) {}
