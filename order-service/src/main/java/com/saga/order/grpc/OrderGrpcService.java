package com.saga.order.grpc;

import com.saga.order.entity.Order;
import com.saga.order.service.OrderService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class OrderGrpcService extends OrderServiceGrpc.OrderServiceImplBase {

    private final OrderService orderService;

    @Override
    public void updateOrderStatus(UpdateOrderStatusRequest request,
                                  StreamObserver<UpdateOrderStatusResponse> responseObserver) {
        log.info("gRPC updateOrderStatus: orderId={}, status={}",
                request.getOrderId(), request.getStatus());
        try {
            Order order = switch (request.getStatus()) {
                case "CONFIRMED" -> orderService.confirmOrder(UUID.fromString(request.getOrderId()));
                case "CANCELLED" -> orderService.cancelOrder(UUID.fromString(request.getOrderId()), "Manual cancellation");
                default -> throw new IllegalArgumentException("Unknown status: " + request.getStatus());
            };

            UpdateOrderStatusResponse response = UpdateOrderStatusResponse.newBuilder()
                    .setOrderId(order.getId().toString())
                    .setStatus(order.getStatus().name())
                    .setMessage("Order status updated successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("updateOrderStatus failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void getOrder(GetOrderRequest request,
                         StreamObserver<GetOrderResponse> responseObserver) {
        log.info("gRPC getOrder: orderId={}", request.getOrderId());
        try {
            Order order = orderService.findById(UUID.fromString(request.getOrderId()));
            GetOrderResponse response = GetOrderResponse.newBuilder()
                    .setOrderId(order.getId().toString())
                    .setUserId(order.getUserId())
                    .setProductId(order.getProductId())
                    .setQuantity(order.getQuantity())
                    .setUnitPrice(order.getUnitPrice().doubleValue())
                    .setTotalAmount(order.getTotalAmount().doubleValue())
                    .setStatus(order.getStatus().name())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (EntityNotFoundException e) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            log.error("getOrder failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
