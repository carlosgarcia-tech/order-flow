package com.orderflow.inventory.service;

import com.orderflow.inventory.domain.Product;
import com.orderflow.inventory.domain.Stock;
import com.orderflow.inventory.domain.StockReservation;
import com.orderflow.inventory.dto.OrderEvent;
import com.orderflow.inventory.event.StockEventPublisher;
import com.orderflow.inventory.repository.ProductRepository;
import com.orderflow.inventory.repository.StockRepository;
import com.orderflow.inventory.repository.StockReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private StockRepository stockRepository;
    @Mock
    private ProductRepository productRepository;
    @Mock
    private StockReservationRepository stockReservationRepository;
    @Mock
    private StockEventPublisher stockEventPublisher;

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(stockRepository, productRepository,
                stockReservationRepository, stockEventPublisher);
    }

    @Test
    void duplicateProductIdInOrderIsRejectedAsAsingleAggregatedDemand() {
        Stock p8 = stock(8L, "RAM", "SKU-8", 200, 0);
        when(stockRepository.findByProductIdForUpdate(8L)).thenReturn(Optional.of(p8));

        OrderEvent event = OrderEvent.builder()
                .orderId(1L)
                .customerId(2L)
                .items(List.of(item(8L, 150), item(8L, 150)))
                .build();

        inventoryService.processOrderCreated(event);

        assertThat(p8.getQuantityReserved()).isZero();
        verify(stockEventPublisher).publishStockRejected(argThat(e -> e.getOrderId().equals(1L)));
        verify(stockEventPublisher, never()).publishStockReserved(any());
        verify(stockReservationRepository, never()).save(any());
    }

    @Test
    void availableOrderReservesQuantityPerProduct() {
        Stock p1 = stock(1L, "Laptop", "SKU-1", 50, 0);
        Stock p2 = stock(2L, "Mouse", "SKU-2", 200, 0);
        when(stockRepository.findByProductIdForUpdate(1L)).thenReturn(Optional.of(p1));
        when(stockRepository.findByProductIdForUpdate(2L)).thenReturn(Optional.of(p2));

        OrderEvent event = OrderEvent.builder()
                .orderId(2L)
                .customerId(3L)
                .items(List.of(item(1L, 2), item(2L, 3)))
                .build();

        inventoryService.processOrderCreated(event);

        assertThat(p1.getQuantityReserved()).isEqualTo(2);
        assertThat(p2.getQuantityReserved()).isEqualTo(3);
        verify(stockEventPublisher).publishStockReserved(any());
        verify(stockEventPublisher, never()).publishStockRejected(any());
        verify(stockReservationRepository, times(2)).save(any());
    }

    @Test
    void insufficientStockIsRejected() {
        Stock p7 = stock(7L, "SSD", "SKU-7", 120, 0);
        when(stockRepository.findByProductIdForUpdate(7L)).thenReturn(Optional.of(p7));

        OrderEvent event = OrderEvent.builder()
                .orderId(3L)
                .customerId(4L)
                .items(List.of(item(7L, 150)))
                .build();

        inventoryService.processOrderCreated(event);

        assertThat(p7.getQuantityReserved()).isZero();
        verify(stockEventPublisher).publishStockRejected(argThat(e -> e.getReason().contains("Insufficient")));
        verify(stockEventPublisher, never()).publishStockReserved(any());
    }

    @Test
    void missingProductIsRejected() {
        when(stockRepository.findByProductIdForUpdate(77L)).thenReturn(Optional.empty());

        OrderEvent event = OrderEvent.builder()
                .orderId(4L)
                .customerId(5L)
                .items(List.of(item(77L, 1)))
                .build();

        inventoryService.processOrderCreated(event);

        verify(stockEventPublisher).publishStockRejected(argThat(e -> e.getReason().contains("not found")));
        verify(stockEventPublisher, never()).publishStockReserved(any());
    }

    @Test
    void releaseReservationsFreesStockAndDeletesReservation() {
        Product p3 = product(3L, "Monitor", "SKU-3");
        Stock s3 = Stock.builder().id(3L).product(p3).quantityAvailable(50).quantityReserved(40).build();
        StockReservation reservation = StockReservation.builder()
                .id(1L)
                .orderId(5L)
                .product(p3)
                .quantity(10)
                .build();
        when(stockReservationRepository.findByOrderId(5L)).thenReturn(List.of(reservation));
        when(stockRepository.findByProductId(3L)).thenReturn(Optional.of(s3));

        inventoryService.releaseReservations(OrderEvent.builder().orderId(5L).build());

        assertThat(s3.getQuantityReserved()).isEqualTo(30);
        verify(stockReservationRepository).delete(reservation);
    }

    @Test
    void getAvailableStockSubtractsReserved() {
        Stock stock = stock(9L, "HDMI", "SKU-9", 100, 30);
        when(stockRepository.findByProductId(9L)).thenReturn(Optional.of(stock));

        assertThat(inventoryService.getAvailableStock(9L)).isEqualTo(70);
    }

    @Test
    void getAvailableStockOfUnknownProductReturnsZero() {
        when(stockRepository.findByProductId(99L)).thenReturn(Optional.empty());

        assertThat(inventoryService.getAvailableStock(99L)).isZero();
    }

    private static OrderEvent.OrderEventItem item(Long productId, Integer quantity) {
        return OrderEvent.OrderEventItem.builder()
                .productId(productId)
                .quantity(quantity)
                .build();
    }

    private static Product product(Long id, String name, String sku) {
        return Product.builder().id(id).name(name).sku(sku).build();
    }

    private static Stock stock(Long productId, String name, String sku,
                               Integer available, Integer reserved) {
        return Stock.builder()
                .product(product(productId, name, sku))
                .quantityAvailable(available)
                .quantityReserved(reserved)
                .build();
    }
}