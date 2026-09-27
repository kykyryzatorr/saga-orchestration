package com.saga.orchestrator.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Serialized as JSON in saga_instances.payload column.
 * Carries the full context needed across all saga steps.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaPayload {

    // Initial request context
    private String userId;
    private String productId;
    private Integer quantity;
    private BigDecimal unitPrice;
    private BigDecimal totalAmount;
    private String currency;

    // Populated as saga progresses
    private UUID orderId;
    private UUID reservationId;
    private UUID paymentId;
}
