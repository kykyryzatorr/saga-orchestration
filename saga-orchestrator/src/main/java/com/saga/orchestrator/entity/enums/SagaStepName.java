package com.saga.orchestrator.entity.enums;

public enum SagaStepName {
    ORDER_CREATE_REQUESTED,
    ORDER_CREATED,
    STOCK_RESERVED,
    PAYMENT_PROCESSED,
    STOCK_RELEASED,     // compensation
    PAYMENT_CANCELLED,  // compensation
    ORDER_CANCELLED,    // compensation
    COMPLETED,
    FAILED
}
