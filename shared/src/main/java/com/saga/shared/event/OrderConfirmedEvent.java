package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Published by saga-orchestrator when all saga steps complete successfully.
 * Consumed by order-service to mark the order as CONFIRMED.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class OrderConfirmedEvent extends BaseEvent {

    private UUID orderId;
    private UUID paymentId;
    private UUID reservationId;
}
