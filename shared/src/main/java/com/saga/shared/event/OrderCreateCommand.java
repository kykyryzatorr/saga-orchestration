package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by order-service.
 * Instructs order-service to create an order with the given (orchestrator-assigned) orderId.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreateCommand extends BaseEvent {

    private UUID orderId;
    private String userId;
    private String productId;
    private Integer quantity;
    private BigDecimal unitPrice;
}
