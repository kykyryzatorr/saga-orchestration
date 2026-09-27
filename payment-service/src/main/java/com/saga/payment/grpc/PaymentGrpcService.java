package com.saga.payment.grpc;

import com.saga.payment.entity.Payment;
import com.saga.payment.service.PaymentService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class PaymentGrpcService extends PaymentServiceGrpc.PaymentServiceImplBase {

    private final PaymentService paymentService;

    @Override
    public void processPayment(PaymentRequest request,
                               StreamObserver<PaymentResponse> responseObserver) {
        log.info("gRPC processPayment: orderId={}, totalAmount={} {}",
                request.getOrderId(), request.getTotalAmount(), request.getCurrency());
        try {
            Payment payment = paymentService.processPayment(
                    null, // sagaId unknown in direct gRPC calls
                    request.getOrderId(),
                    request.getUserId(),
                    BigDecimal.valueOf(request.getTotalAmount()),
                    request.getCurrency(),
                    null // no idempotency key on the direct gRPC path
            );

            PaymentResponse response = PaymentResponse.newBuilder()
                    .setPaymentId(payment.getId().toString())
                    .setStatus(payment.getStatus().name())
                    .setMessage("Payment processed")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("processPayment failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void cancelPayment(CancelPaymentRequest request,
                              StreamObserver<CancelPaymentResponse> responseObserver) {
        log.info("gRPC cancelPayment: paymentId={}, reason={}",
                request.getPaymentId(), request.getReason());
        try {
            Payment payment = paymentService.cancelPayment(
                    null, // sagaId unknown in direct gRPC calls
                    UUID.fromString(request.getPaymentId()),
                    request.getReason()
            );

            CancelPaymentResponse response = CancelPaymentResponse.newBuilder()
                    .setPaymentId(payment.getId().toString())
                    .setStatus(payment.getStatus().name())
                    .setMessage("Payment cancelled")
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("cancelPayment failed: {}", e.getMessage(), e);
            responseObserver.onError(
                    Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}
