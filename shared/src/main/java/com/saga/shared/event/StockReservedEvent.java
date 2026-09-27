package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by inventory-service when stock is successfully reserved.
 * Consumed by saga-orchestrator to proceed to the payment step.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReservedEvent extends BaseEvent {

    private UUID orderId;
    private UUID reservationId;
    private UUID productId;
    private Integer quantity;
}
