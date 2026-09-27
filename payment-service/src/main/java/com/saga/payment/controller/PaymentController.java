package com.saga.payment.controller;

import com.saga.payment.dto.PaymentDto;
import com.saga.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    /**
     * GET /api/payments/{paymentId}
     * Returns a single payment by its ID.
     */
    @GetMapping("/{paymentId}")
    public ResponseEntity<PaymentDto> getPayment(@PathVariable UUID paymentId) {
        log.info("GET /api/payments/{}", paymentId);
        return ResponseEntity.ok(PaymentDto.from(paymentService.findById(paymentId)));
    }

    /**
     * GET /api/payments/order/{orderId}
     * Returns the payment associated with a given order.
     */
    @GetMapping("/order/{orderId}")
    public ResponseEntity<PaymentDto> getPaymentByOrder(@PathVariable String orderId) {
        log.info("GET /api/payments/order/{}", orderId);
        return ResponseEntity.ok(PaymentDto.from(paymentService.findByOrderId(orderId)));
    }
}
