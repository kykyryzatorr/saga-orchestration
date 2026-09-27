package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by inventory-service.
 * Instructs inventory-service to reserve stock for an order.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReserveCommand extends BaseEvent {

    private UUID orderId;
    private UUID productId;
    private Integer quantity;
}
