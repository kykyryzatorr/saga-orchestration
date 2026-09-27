package com.saga.shared.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.NoArgsConstructor;

import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Published by payment-service when payment is successfully processed.
 * Consumed by saga-orchestrator to complete the saga.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentProcessedEvent extends BaseEvent {

    private UUID orderId;
    private UUID paymentId;
    private BigDecimal totalAmount;
    private String currency;
}
