package com.saga.orchestrator.dto;

import java.util.UUID;

public record StartOrderResponse(
        UUID sagaId,
        String orderId,
        String status,
        String message
) {}
