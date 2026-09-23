package com.orderflow.inventory.listener;

import com.orderflow.inventory.dto.OrderEvent;
import com.orderflow.inventory.service.InventoryService;
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
public class OrderCreatedListener {

    private final InventoryService inventoryService;

    @KafkaListener(topics = "order-created", groupId = "inventory-service")
    public void handleOrderCreated(@Payload OrderEvent event,
                                    @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        log.info("Received OrderCreated event for order: {}", event.getOrderId());
        inventoryService.processOrderCreated(event);
    }
}
