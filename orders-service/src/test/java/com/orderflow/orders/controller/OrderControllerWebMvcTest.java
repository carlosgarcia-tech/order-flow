package com.orderflow.orders.controller;

import com.orderflow.orders.config.SecurityConfig;
import com.orderflow.orders.domain.OrderStatus;
import com.orderflow.orders.dto.OrderResponse;
import com.orderflow.orders.exception.OrderNotFoundException;
import com.orderflow.orders.security.JwtAuthenticationFilter;
import com.orderflow.orders.security.JwtUtil;
import com.orderflow.orders.service.OrderService;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import javax.crypto.SecretKey;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
@Import({SecurityConfig.class, JwtAuthenticationFilter.class, JwtUtil.class})
@ActiveProfiles("test")
class OrderControllerWebMvcTest {

    private static final String SECRET = "mySecretKeyThatIsAtLeast32BytesLongForHS256Algorithm!";
    private static final String CREATE_BODY =
            "{\"items\":[{\"productId\":9,\"quantity\":1}]}";

    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private OrderService orderService;

    @Test
    void unauthenticatedRequestIsUnauthorized() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CREATE_BODY))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void createOrderWithUserRoleReturnsCreated() throws Exception {
        OrderResponse response = orderResponse(1L, "alice", OrderStatus.PENDING);
        when(orderService.createOrder(any(), any(), any())).thenReturn(response);

        mockMvc.perform(post("/api/orders")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CREATE_BODY))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void createOrderWithAdminRoleIsForbidden() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .header("Authorization", bearer("boss", "ROLE_ADMIN", 1L))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(CREATE_BODY))
                .andExpect(status().isForbidden());
    }

    @Test
    void getOrderOwnedByCurrentUserReturnsIt() throws Exception {
        when(orderService.getOrder(5L)).thenReturn(orderResponse(5L, "alice", OrderStatus.CONFIRMED));

        mockMvc.perform(get("/api/orders/5")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("alice"));
    }

    @Test
    void getOrderOwnedByAnotherUserIsForbidden() throws Exception {
        when(orderService.getOrder(5L)).thenReturn(orderResponse(5L, "alice", OrderStatus.CONFIRMED));

        mockMvc.perform(get("/api/orders/5")
                        .header("Authorization", bearer("bob", "ROLE_USER", 8L)))
                .andExpect(status().isForbidden());
    }

    @Test
    void getOrderWithNonNumericIdIsBadRequest() throws Exception {
        mockMvc.perform(get("/api/orders/abc")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getMissingOrderIsNotFound() throws Exception {
        when(orderService.getOrder(999L)).thenThrow(new OrderNotFoundException(999L));

        mockMvc.perform(get("/api/orders/999")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isNotFound());
    }

    @Test
    void cancelOwnOrderReturnsCancelled() throws Exception {
        when(orderService.getOrder(5L)).thenReturn(orderResponse(5L, "alice", OrderStatus.PENDING));
        when(orderService.cancelOrder(5L)).thenReturn(orderResponse(5L, "alice", OrderStatus.CANCELLED));

        mockMvc.perform(put("/api/orders/5/cancel")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void listOrdersReturnsList() throws Exception {
        when(orderService.getOrdersByUsername("alice"))
                .thenReturn(List.of(orderResponse(1L, "alice", OrderStatus.CONFIRMED)));

        mockMvc.perform(get("/api/orders")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }

    @Test
    void unsupportedHttpMethodReturnsNotAllowed() throws Exception {
        mockMvc.perform(delete("/api/orders/5")
                        .header("Authorization", bearer("alice", "ROLE_USER", 7L)))
                .andExpect(status().isMethodNotAllowed());
    }

    @Test
    void tamperedTokenIsUnauthorized() throws Exception {
        String valid = bearer("alice", "ROLE_USER", 7L);
        String tampered = valid.substring(0, valid.length() - 1) + "X";

        mockMvc.perform(get("/api/orders")
                        .header("Authorization", tampered))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void expiredTokenIsUnauthorized() throws Exception {
        SecretKey key = Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8));
        String token = Jwts.builder()
                .subject("alice")
                .claim("role", "ROLE_USER")
                .claim("userId", 7L)
                .issuedAt(new Date(System.currentTimeMillis() - 10_000))
                .expiration(new Date(System.currentTimeMillis() - 5_000))
                .signWith(key)
                .compact();

        mockMvc.perform(get("/api/orders")
                        .header("Authorization", "Bearer " + token))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void algorithmNoneTokenIsUnauthorized() throws Exception {
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url("{\"sub\":\"alice\",\"role\":\"ROLE_USER\",\"userId\":7,\"exp\":4102444800}");

        mockMvc.perform(get("/api/orders")
                        .header("Authorization", "Bearer " + header + "." + payload + "."))
                .andExpect(status().isUnauthorized());
    }

    private static OrderResponse orderResponse(Long id, String username, OrderStatus status) {
        return OrderResponse.builder()
                .id(id)
                .customerId(7L)
                .username(username)
                .status(status)
                .total(new BigDecimal("99.99"))
                .items(List.of())
                .build();
    }

    private static String bearer(String username, String role, Long userId) {
        SecretKey key = Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8));
        return "Bearer " + Jwts.builder()
                .subject(username)
                .claim("role", role)
                .claim("userId", userId)
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 3_600_000))
                .signWith(key)
                .compact();
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}