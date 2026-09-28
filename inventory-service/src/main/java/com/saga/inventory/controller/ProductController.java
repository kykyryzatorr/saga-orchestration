package com.saga.inventory.controller;

import com.saga.inventory.dto.CreateProductRequest;
import com.saga.inventory.dto.ProductDto;
import com.saga.inventory.service.InventoryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {

    private final InventoryService inventoryService;

    /**
     * POST /api/products
     * Adds a new product with initial stock. Useful for test setup.
     */
    @PostMapping
    public ResponseEntity<ProductDto> createProduct(@Valid @RequestBody CreateProductRequest request) {
        log.info("POST /api/products: name={}, quantity={}", request.name(), request.quantity());
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(ProductDto.from(inventoryService.addProduct(request.name(), request.quantity())));
    }

    /**
     * GET /api/products
     * Lists all products with current stock levels.
     */
    @GetMapping
    public ResponseEntity<List<ProductDto>> getAllProducts() {
        log.info("GET /api/products");
        List<ProductDto> products = inventoryService.findAllProducts()
                .stream()
                .map(ProductDto::from)
                .toList();
        return ResponseEntity.ok(products);
    }

    /**
     * GET /api/products/{productId}
     * Returns a single product with current stock levels.
     */
    @GetMapping("/{productId}")
    public ResponseEntity<ProductDto> getProduct(@PathVariable UUID productId) {
        log.info("GET /api/products/{}", productId);
        return ResponseEntity.ok(ProductDto.from(inventoryService.findProductById(productId)));
    }
}
