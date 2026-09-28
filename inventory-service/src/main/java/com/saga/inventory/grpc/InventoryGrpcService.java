package com.saga.inventory.grpc;

import com.saga.inventory.entity.Product;
import com.saga.inventory.entity.StockReservation;
import com.saga.inventory.service.InventoryService;
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
public class InventoryGrpcService extends InventoryServiceGrpc.InventoryServiceImplBase {

    private final InventoryService inventoryService;

    @Override
    public void reserveStock(ReserveStockRequest request,
                             StreamObserver<ReserveStockResponse> responseObserver) {
        log.info("gRPC reserveStock: orderId={}, productId={}, quantity={}",
                request.getOrderId(), request.getProductId(), request.getQuantity());
        try {
            StockReservation reservation = inventoryService.reserveStock(
                    null, // sagaId unknown in direct gRPC calls
                    request.getOrderId(),
                    UUID.fromString(request.getProductId()),
                    request.getQuantity(),
                    null // no idempotency key on the direct gRPC path
            );

            ReserveStockResponse response = ReserveStockResponse.newBuilder()
                    .setReservationId(reservation.getId().toString())
                    .setStatus(reservation.getStatus().name())
                    .setMessage("Stock reserved successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IllegalStateException e) {
            // Insufficient stock — surface as gRPC FAILED_PRECONDITION
            log.warn("reserveStock precondition failed: {}", e.getMessage());
            responseObserver.onError(
                    Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            log.error("reserveStock failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void releaseStock(ReleaseStockRequest request,
                             StreamObserver<ReleaseStockResponse> responseObserver) {
        log.info("gRPC releaseStock: reservationId={}, reason={}",
                request.getReservationId(), request.getReason());
        try {
            StockReservation reservation = inventoryService.releaseStock(
                    null, // sagaId unknown in direct gRPC calls
                    UUID.fromString(request.getReservationId()),
                    request.getReason()
            );

            ReleaseStockResponse response = ReleaseStockResponse.newBuilder()
                    .setReservationId(reservation.getId().toString())
                    .setStatus(reservation.getStatus().name())
                    .setMessage("Stock released successfully")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("releaseStock failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void getProduct(GetProductRequest request,
                           StreamObserver<GetProductResponse> responseObserver) {
        log.info("gRPC getProduct: productId={}", request.getProductId());
        try {
            Product product = inventoryService.findProductById(UUID.fromString(request.getProductId()));
            GetProductResponse response = GetProductResponse.newBuilder()
                    .setProductId(product.getId().toString())
                    .setName(product.getName())
                    .setTotalQuantity(product.getQuantity())
                    .setReservedQuantity(product.getReservedQuantity())
                    .setAvailableQuantity(product.getAvailableQuantity())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (EntityNotFoundException e) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            log.error("getProduct failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void checkStock(CheckStockRequest request,
                           StreamObserver<CheckStockResponse> responseObserver) {
        log.info("gRPC checkStock: productId={}, quantity={}", request.getProductId(), request.getQuantity());
        try {
            Product product = inventoryService.findProductById(UUID.fromString(request.getProductId()));
            int available = product.getAvailableQuantity();
            boolean sufficient = available >= request.getQuantity();
            CheckStockResponse response = CheckStockResponse.newBuilder()
                    .setAvailable(sufficient)
                    .setAvailableQuantity(available)
                    .setMessage(sufficient
                            ? "Stock available: " + available + " units"
                            : "Insufficient stock: requested=" + request.getQuantity() + ", available=" + available)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (EntityNotFoundException e) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            log.error("checkStock failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
