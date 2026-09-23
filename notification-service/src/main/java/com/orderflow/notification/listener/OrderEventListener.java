package com.orderflow.notification.listener;

import com.orderflow.notification.dto.OrderEvent;
import com.orderflow.notification.service.NotificationService;
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
public class OrderEventListener {

    private final NotificationService notificationService;

    @KafkaListener(topics = "order-created", groupId = "notification-service")
    public void handleOrderCreated(@Payload OrderEvent event,
                                    @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received OrderCreated event for order: {}", event.getOrderId());
        notificationService.sendOrderCreatedNotification(event.getOrderId(), event.getCustomerId());
    }

    @KafkaListener(topics = "order-cancelled", groupId = "notification-service")
    public void handleOrderCancelled(@Payload OrderEvent event,
                                      @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received OrderCancelled event for order: {}", event.getOrderId());
        notificationService.sendOrderCancelledNotification(event.getOrderId(), event.getCustomerId());
    }

    @KafkaListener(topics = "stock-reserved", groupId = "notification-service")
    public void handleStockReserved(@Payload OrderEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received StockReserved event for order: {}", event.getOrderId());
        notificationService.sendStockReservedNotification(event.getOrderId());
    }
}
