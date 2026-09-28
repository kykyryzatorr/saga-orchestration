package com.saga.order.repository;

import com.saga.order.entity.Order;
import com.saga.order.entity.enums.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    List<Order> findByUserId(String userId);

    List<Order> findByStatus(OrderStatus status);

    Optional<Order> findByIdAndStatus(UUID id, OrderStatus status);

    Optional<Order> findByIdempotencyKey(String idempotencyKey);
}
