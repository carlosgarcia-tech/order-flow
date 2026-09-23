package com.orderflow.inventory.controller;

import com.orderflow.inventory.service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    @GetMapping("/{productId}/stock")
    public ResponseEntity<Map<String, Object>> getStock(@PathVariable Long productId) {
        Integer available = inventoryService.getAvailableStock(productId);
        return ResponseEntity.ok(Map.of(
                "productId", productId,
                "availableQuantity", available));
    }
}
