package com.saga.payment.listener;

import com.saga.payment.service.PaymentService;
import com.saga.shared.event.PaymentCancelCommand;
import com.saga.shared.event.PaymentProcessCommand;
import com.saga.shared.kafka.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    private final PaymentService paymentService;

    @KafkaListener(topics = KafkaTopics.PAYMENT_PROCESS_REQUEST,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentProcessRequest(PaymentProcessCommand cmd) {
        log.info("← payment.process.request: sagaId={}, orderId={}, totalAmount={} {}",
                cmd.getSagaId(), cmd.getOrderId(), cmd.getTotalAmount(), cmd.getCurrency());
        paymentService.processPayment(
                cmd.getSagaId(),
                cmd.getOrderId().toString(),
                cmd.getUserId(),
                cmd.getTotalAmount(),
                cmd.getCurrency(),
                cmd.getIdempotencyKey()
        );
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_CANCEL_REQUEST,
                   groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCancelRequest(PaymentCancelCommand cmd) {
        log.info("← payment.cancel.request: sagaId={}, paymentId={}, reason={}",
                cmd.getSagaId(), cmd.getPaymentId(), cmd.getReason());
        paymentService.cancelPayment(
                cmd.getSagaId(),
                cmd.getPaymentId(),
                cmd.getReason()
        );
    }
}
