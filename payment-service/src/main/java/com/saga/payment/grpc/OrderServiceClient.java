package com.saga.payment.grpc;

import com.saga.order.grpc.GetOrderRequest;
import com.saga.order.grpc.GetOrderResponse;
import com.saga.order.grpc.OrderServiceGrpc;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderServiceClient {

    // Retries only transient gRPC failures (UNAVAILABLE, DEADLINE_EXCEEDED) — read-only, safe to retry.
    private static final String RETRYABLE_GRPC_STATUS =
            "#root instanceof T(io.grpc.StatusRuntimeException) && (" +
                    "#root.status.code.name() == 'UNAVAILABLE' || " +
                    "#root.status.code.name() == 'DEADLINE_EXCEEDED')";

    @GrpcClient("order-service")
    private OrderServiceGrpc.OrderServiceBlockingStub orderServiceStub;

    @Retryable(exceptionExpression = RETRYABLE_GRPC_STATUS,
               maxAttempts = 3, backoff = @Backoff(delay = 200, multiplier = 2))
    public GetOrderResponse getOrder(String orderId) {
        log.info("gRPC → order-service.getOrder: orderId={}", orderId);
        return orderServiceStub.getOrder(
                GetOrderRequest.newBuilder()
                        .setOrderId(orderId)
                        .build()
        );
    }
}
