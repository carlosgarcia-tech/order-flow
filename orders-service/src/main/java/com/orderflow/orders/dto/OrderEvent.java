package com.orderflow.orders.dto;

import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    private Long orderId;
    private Long customerId;
    private String username;
    private List<OrderEventItem> items;
    private LocalDateTime timestamp;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderEventItem {
        private Long productId;
        private Integer quantity;
    }
}
