package com.saga.order.controller;

import com.saga.order.dto.OrderDto;
import com.saga.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * GET /api/orders/{orderId}
     * Returns the current state of a single order.
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderDto> getOrder(@PathVariable UUID orderId) {
        log.info("GET /api/orders/{}", orderId);
        return ResponseEntity.ok(OrderDto.from(orderService.findById(orderId)));
    }

    /**
     * GET /api/orders?userId={userId}
     * Returns all orders belonging to a user.
     */
    @GetMapping
    public ResponseEntity<List<OrderDto>> getOrdersByUser(@RequestParam String userId) {
        log.info("GET /api/orders?userId={}", userId);
        List<OrderDto> orders = orderService.findByUserId(userId)
                .stream()
                .map(OrderDto::from)
                .toList();
        return ResponseEntity.ok(orders);
    }
}
