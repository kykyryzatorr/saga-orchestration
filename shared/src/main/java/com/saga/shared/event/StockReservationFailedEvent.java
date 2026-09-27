package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by inventory-service when stock reservation fails (e.g. insufficient stock).
 * Consumed by saga-orchestrator to trigger compensation (cancel the order).
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class StockReservationFailedEvent extends BaseEvent {

    private UUID orderId;
    private UUID productId;
    private Integer requestedQuantity;
    private Integer availableQuantity;
    private String reason;
}
