package com.orderflow.orders.event;

import com.orderflow.orders.dto.OrderEvent;
import com.orderflow.orders.dto.StockEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrderCreated(OrderEvent event) {
        log.info("Publishing OrderCreated event for order: {}", event.getOrderId());
        kafkaTemplate.send("order-created", event.getOrderId().toString(), event);
    }

    public void publishOrderCancelled(OrderEvent event) {
        log.info("Publishing OrderCancelled event for order: {}", event.getOrderId());
        kafkaTemplate.send("order-cancelled", event.getOrderId().toString(), event);
    }
}
