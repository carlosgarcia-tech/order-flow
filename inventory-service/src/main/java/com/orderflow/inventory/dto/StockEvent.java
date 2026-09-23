package com.orderflow.inventory.dto;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockEvent {

    private Long orderId;
    private String reason;
    private LocalDateTime timestamp;
}
