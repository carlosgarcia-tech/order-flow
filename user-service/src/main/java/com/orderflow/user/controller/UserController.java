package com.orderflow.user.controller;

import com.orderflow.user.dto.AuthResponse;
import com.orderflow.user.dto.LoginRequest;
import com.orderflow.user.dto.RegisterRequest;
import com.orderflow.user.service.UserService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/auth/register")
    public ResponseEntity<AuthResponse> register(@Valid @RequestBody RegisterRequest request) {
        AuthResponse response = userService.register(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PostMapping("/auth/login")
    public ResponseEntity<AuthResponse> login(@Valid @RequestBody LoginRequest request) {
        AuthResponse response = userService.login(request);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/users/{id}")
    @PreAuthorize("hasRole('USER')")
    public ResponseEntity<com.orderflow.user.dto.UserResponse> getUser(@PathVariable Long id) {
        com.orderflow.user.dto.UserResponse response = userService.getUserById(id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/users")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<List<com.orderflow.user.dto.UserResponse>> getAllUsers() {
        List<com.orderflow.user.dto.UserResponse> responses = userService.getAllUsers();
        return ResponseEntity.ok(responses);
    }

    @GetMapping("/users/me")
    @PreAuthorize("hasRole('USER')")
    public ResponseEntity<com.orderflow.user.dto.UserResponse> getCurrentUser(Authentication authentication) {
        com.orderflow.user.dto.UserResponse response = userService.getUserByUsername(authentication.getName());
        return ResponseEntity.ok(response);
    }
}
