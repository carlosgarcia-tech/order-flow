package com.orderflow.orders.service;

import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderStatus;
import com.orderflow.orders.dto.CreateOrderRequest;
import com.orderflow.orders.dto.OrderResponse;
import com.orderflow.orders.dto.StockEvent;
import com.orderflow.orders.event.OrderEventPublisher;
import com.orderflow.orders.exception.IllegalOrderStateException;
import com.orderflow.orders.exception.OrderNotFoundException;
import com.orderflow.orders.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;
    @Mock
    private OrderEventPublisher orderEventPublisher;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, orderEventPublisher);
    }

    @Test
    void createOrderComputesTotalAndPublishesEvent() {
        CreateOrderRequest request = CreateOrderRequest.builder()
                .items(List.of(
                        CreateOrderRequest.OrderItemRequest.builder().productId(1L).quantity(2).build(),
                        CreateOrderRequest.OrderItemRequest.builder().productId(2L).quantity(3).build()))
                .build();
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setId(1L);
            return order;
        });

        OrderResponse response = orderService.createOrder(request, "alice", 7L);

        assertThat(response.getId()).isEqualTo(1L);
        assertThat(response.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(response.getTotal()).isEqualByComparingTo("499.95");
        assertThat(response.getItems()).hasSize(2);
        assertThat(response.getUsername()).isEqualTo("alice");
        verify(orderEventPublisher).publishOrderCreated(any());
    }

    @Test
    void getOrderThrowsWhenMissing() {
        when(orderRepository.findById(99L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder(99L))
                .isInstanceOf(OrderNotFoundException.class);
    }

    @Test
    void cancelOrderPublishesSingleCancellationEvent() {
        Order pending = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("99.99"))
                .build();
        Order cancelled = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.CANCELLED)
                .total(new BigDecimal("99.99"))
                .updatedAt(LocalDateTime.now())
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(pending), Optional.of(cancelled));
        when(orderRepository.markStatusIfActive(eq(5L), eq(OrderStatus.CANCELLED), any())).thenReturn(1);

        OrderResponse response = orderService.cancelOrder(5L);

        assertThat(response.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderEventPublisher, times(1)).publishOrderCancelled(any());
    }

    @Test
    void cancelOrderOnAlreadyCancelledThrows() {
        Order cancelled = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.CANCELLED)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(cancelled));

        assertThatThrownBy(() -> orderService.cancelOrder(5L))
                .isInstanceOf(IllegalOrderStateException.class);
        verify(orderEventPublisher, never()).publishOrderCancelled(any());
    }

    @Test
    void cancelOrderLosingTransitionRaceThrows() {
        Order pending = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(pending));
        when(orderRepository.markStatusIfActive(eq(5L), eq(OrderStatus.CANCELLED), any())).thenReturn(0);

        assertThatThrownBy(() -> orderService.cancelOrder(5L))
                .isInstanceOf(IllegalOrderStateException.class);
        verify(orderEventPublisher, never()).publishOrderCancelled(any());
    }

    @Test
    void handleStockReservedConfirmsPendingOrder() {
        Order pending = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(pending));

        orderService.handleStockReserved(StockEvent.builder().orderId(5L).build());

        assertThat(pending.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        verify(orderRepository).save(pending);
    }

    @Test
    void handleStockReservedIgnoresCancelledOrder() {
        Order cancelled = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.CANCELLED)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(cancelled));

        orderService.handleStockReserved(StockEvent.builder().orderId(5L).build());

        verify(orderRepository, never()).save(any());
    }

    @Test
    void handleStockRejectedCancelsAndPublishes() {
        Order pending = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(pending));

        orderService.handleStockRejected(StockEvent.builder().orderId(5L).reason("No stock").build());

        assertThat(pending.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        verify(orderRepository).save(pending);
        verify(orderEventPublisher, times(1)).publishOrderCancelled(any());
    }

    @Test
    void handleStockRejectedIgnoresAlreadyCancelled() {
        Order cancelled = Order.builder()
                .id(5L)
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.CANCELLED)
                .total(new BigDecimal("99.99"))
                .build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(cancelled));

        orderService.handleStockRejected(StockEvent.builder().orderId(5L).reason("No stock").build());

        verify(orderRepository, never()).save(any());
        verify(orderEventPublisher, never()).publishOrderCancelled(any());
    }
}