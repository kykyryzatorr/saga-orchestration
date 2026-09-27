package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.util.UUID;

/**
 * Command published by saga-orchestrator → consumed by payment-service (compensation).
 * Instructs payment-service to cancel a previously processed payment.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentCancelCommand extends BaseEvent {

    private UUID orderId;
    private UUID paymentId;
    private String reason;
}
