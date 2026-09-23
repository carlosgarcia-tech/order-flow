package com.orderflow.inventory.service;

import com.orderflow.inventory.domain.Product;
import com.orderflow.inventory.domain.Stock;
import com.orderflow.inventory.domain.StockReservation;
import com.orderflow.inventory.dto.OrderEvent;
import com.orderflow.inventory.dto.StockEvent;
import com.orderflow.inventory.event.StockEventPublisher;
import com.orderflow.inventory.repository.ProductRepository;
import com.orderflow.inventory.repository.StockRepository;
import com.orderflow.inventory.repository.StockReservationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final StockRepository stockRepository;
    private final ProductRepository productRepository;
    private final StockReservationRepository stockReservationRepository;
    private final StockEventPublisher stockEventPublisher;

    @Transactional
    public void processOrderCreated(OrderEvent event) {
        log.info("Processing OrderCreated event for order: {}", event.getOrderId());

        Map<Long, Integer> totalByProduct = new LinkedHashMap<>();
        for (OrderEvent.OrderEventItem item : event.getItems()) {
            totalByProduct.merge(item.getProductId(), item.getQuantity(), Integer::sum);
        }

        boolean allItemsAvailable = true;
        StringBuilder rejectReason = new StringBuilder();
        Map<Long, Stock> stockByProduct = new HashMap<>();

        for (Map.Entry<Long, Integer> entry : totalByProduct.entrySet()) {
            Stock stock = stockRepository.findByProductIdForUpdate(entry.getKey()).orElse(null);

            if (stock == null) {
                allItemsAvailable = false;
                rejectReason.append("Product ").append(entry.getKey()).append(" not found. ");
                break;
            }

            stockByProduct.put(entry.getKey(), stock);
            int available = stock.getQuantityAvailable() - stock.getQuantityReserved();

            if (available < entry.getValue()) {
                allItemsAvailable = false;
                rejectReason.append("Insufficient stock for product ")
                        .append(entry.getKey())
                        .append(". Available: ").append(available)
                        .append(", Requested: ").append(entry.getValue()).append(". ");
            }
        }

        if (allItemsAvailable) {
            for (Map.Entry<Long, Integer> entry : totalByProduct.entrySet()) {
                Stock stock = stockByProduct.get(entry.getKey());
                stock.setQuantityReserved(stock.getQuantityReserved() + entry.getValue());
                stockRepository.save(stock);
                stockReservationRepository.save(StockReservation.builder()
                        .orderId(event.getOrderId())
                        .product(stock.getProduct())
                        .quantity(entry.getValue())
                        .build());
                log.info("Reserved {} units of product {} for order {}",
                        entry.getValue(), entry.getKey(), event.getOrderId());
            }

            stockEventPublisher.publishStockReserved(StockEvent.builder()
                    .orderId(event.getOrderId())
                    .timestamp(LocalDateTime.now())
                    .build());
        } else {
            stockEventPublisher.publishStockRejected(StockEvent.builder()
                    .orderId(event.getOrderId())
                    .reason(rejectReason.toString().trim())
                    .timestamp(LocalDateTime.now())
                    .build());
        }
    }

    @Transactional
    public void releaseReservations(OrderEvent event) {
        Long orderId = event.getOrderId();
        log.info("Releasing stock reservations for cancelled order: {}", orderId);

        for (StockReservation reservation : stockReservationRepository.findByOrderId(orderId)) {
            Long productId = reservation.getProduct().getId();
            stockRepository.findByProductId(productId).ifPresent(stock -> {
                int released = Math.min(stock.getQuantityReserved(), reservation.getQuantity());
                stock.setQuantityReserved(stock.getQuantityReserved() - released);
                stockRepository.save(stock);
                log.info("Released {} units of product {} for cancelled order {}",
                        released, productId, orderId);
            });
            stockReservationRepository.delete(reservation);
        }
    }

    @Transactional(readOnly = true)
    public Integer getAvailableStock(Long productId) {
        return stockRepository.findByProductId(productId)
                .map(stock -> stock.getQuantityAvailable() - stock.getQuantityReserved())
                .orElse(0);
    }
}
