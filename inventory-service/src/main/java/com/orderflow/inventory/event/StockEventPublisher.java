package com.orderflow.inventory.event;

import com.orderflow.inventory.dto.StockEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishStockReserved(StockEvent event) {
        log.info("Publishing StockReserved event for order: {}", event.getOrderId());
        kafkaTemplate.send("stock-reserved", event.getOrderId().toString(), event);
    }

    public void publishStockRejected(StockEvent event) {
        log.info("Publishing StockRejected event for order: {}", event.getOrderId());
        kafkaTemplate.send("stock-rejected", event.getOrderId().toString(), event);
    }
}
