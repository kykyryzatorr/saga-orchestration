package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by saga-orchestrator during compensation.
 * Consumed by order-service to mark the order as CANCELLED.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCancelledEvent extends BaseEvent {

    private UUID orderId;
    private String reason;
}
