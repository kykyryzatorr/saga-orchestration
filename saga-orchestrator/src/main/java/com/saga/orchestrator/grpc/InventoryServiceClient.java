package com.saga.orchestrator.grpc;

import com.saga.inventory.grpc.CheckStockRequest;
import com.saga.inventory.grpc.CheckStockResponse;
import com.saga.inventory.grpc.GetProductRequest;
import com.saga.inventory.grpc.GetProductResponse;
import com.saga.inventory.grpc.InventoryServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InventoryServiceClient {

    // Retries only transient gRPC failures (UNAVAILABLE, DEADLINE_EXCEEDED) — other status
    // codes (e.g. INVALID_ARGUMENT) are not transient and fail immediately.
    private static final String RETRYABLE_GRPC_STATUS =
            "#root instanceof T(io.grpc.StatusRuntimeException) && (" +
                    "#root.status.code.name() == 'UNAVAILABLE' || " +
                    "#root.status.code.name() == 'DEADLINE_EXCEEDED')";

    @GrpcClient("inventory-service")
    private InventoryServiceGrpc.InventoryServiceBlockingStub inventoryServiceStub;

    // Advisory check only — does not reserve. The actual reservation uses pessimistic locking.
    @Retryable(exceptionExpression = RETRYABLE_GRPC_STATUS,
               maxAttempts = 3, backoff = @Backoff(delay = 200, multiplier = 2))
    public CheckStockResponse checkStock(String productId, int quantity) {
        log.info("gRPC → inventory-service.checkStock: productId={}, quantity={}", productId, quantity);
        return inventoryServiceStub.checkStock(
                CheckStockRequest.newBuilder()
                        .setProductId(productId)
                        .setQuantity(quantity)
                        .build()
        );
    }

    @Retryable(exceptionExpression = RETRYABLE_GRPC_STATUS,
               maxAttempts = 3, backoff = @Backoff(delay = 200, multiplier = 2))
    public GetProductResponse getProduct(String productId) {
        log.info("gRPC → inventory-service.getProduct: productId={}", productId);
        return inventoryServiceStub.getProduct(
                GetProductRequest.newBuilder()
                        .setProductId(productId)
                        .build()
        );
    }
}
