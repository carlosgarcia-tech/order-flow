package com.orderflow.orders.controller;

import com.orderflow.orders.dto.CreateOrderRequest;
import com.orderflow.orders.dto.OrderResponse;
import com.orderflow.orders.security.AuthenticatedUser;
import com.orderflow.orders.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request,
                                                      Authentication authentication) {
        AuthenticatedUser user = currentUser(authentication);
        OrderResponse response = orderService.createOrder(request, user.username(), user.id());
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable Long id, Authentication authentication) {
        OrderResponse response = orderService.getOrder(id);
        assertOwnership(response, currentUser(authentication));
        return ResponseEntity.ok(response);
    }

    @GetMapping
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<List<OrderResponse>> getOrders(Authentication authentication) {
        List<OrderResponse> responses = orderService.getOrdersByUsername(currentUser(authentication).username());
        return ResponseEntity.ok(responses);
    }

    @PutMapping("/{id}/cancel")
    @PreAuthorize("hasRole('ROLE_USER')")
    public ResponseEntity<OrderResponse> cancelOrder(@PathVariable Long id, Authentication authentication) {
        AuthenticatedUser user = currentUser(authentication);
        OrderResponse response = orderService.getOrder(id);
        assertOwnership(response, user);
        OrderResponse cancelled = orderService.cancelOrder(id);
        return ResponseEntity.ok(cancelled);
    }

    private AuthenticatedUser currentUser(Authentication authentication) {
        return (AuthenticatedUser) authentication.getPrincipal();
    }

    private void assertOwnership(OrderResponse order, AuthenticatedUser user) {
        if (!"ROLE_ADMIN".equals(user.role()) && !order.getUsername().equals(user.username())) {
            throw new AccessDeniedException("Cannot access another user's order");
        }
    }
}
