package com.orderflow.orders.security;

public record AuthenticatedUser(Long id, String username, String role) {
}