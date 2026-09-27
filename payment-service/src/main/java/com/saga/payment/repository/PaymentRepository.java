package com.saga.payment.repository;

import com.saga.payment.entity.Payment;
import com.saga.payment.entity.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByOrderId(String orderId);

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    List<Payment> findByUserId(String userId);

    List<Payment> findByStatus(PaymentStatus status);
}
