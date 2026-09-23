package com.orderflow.orders.listener;

import com.orderflow.orders.dto.StockEvent;
import com.orderflow.orders.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockEventListener {

    private final OrderService orderService;

    @KafkaListener(topics = "stock-reserved", groupId = "orders-service")
    public void handleStockReserved(@Payload StockEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received StockReserved event for order: {}", event.getOrderId());
        orderService.handleStockReserved(event);
    }

    @KafkaListener(topics = "stock-rejected", groupId = "orders-service")
    public void handleStockRejected(@Payload StockEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received StockRejected event for order: {}", event.getOrderId());
        orderService.handleStockRejected(event);
    }
}
