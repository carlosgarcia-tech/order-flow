package com.orderflow.orders.service;

import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderItem;
import com.orderflow.orders.domain.OrderStatus;
import com.orderflow.orders.dto.CreateOrderRequest;
import com.orderflow.orders.dto.OrderEvent;
import com.orderflow.orders.dto.OrderResponse;
import com.orderflow.orders.dto.StockEvent;
import com.orderflow.orders.event.OrderEventPublisher;
import com.orderflow.orders.exception.IllegalOrderStateException;
import com.orderflow.orders.exception.OrderNotFoundException;
import com.orderflow.orders.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request, String username, Long customerId) {
        Order order = Order.builder()
                .customerId(customerId)
                .username(username)
                .status(OrderStatus.PENDING)
                .build();

        BigDecimal total = BigDecimal.ZERO;
        for (CreateOrderRequest.OrderItemRequest itemReq : request.getItems()) {
            BigDecimal itemPrice = BigDecimal.valueOf(99.99);
            OrderItem item = OrderItem.builder()
                    .productId(itemReq.getProductId())
                    .quantity(itemReq.getQuantity())
                    .price(itemPrice)
                    .build();
            order.addItem(item);
            total = total.add(itemPrice.multiply(BigDecimal.valueOf(itemReq.getQuantity())));
        }
        order.setTotal(total);

        Order savedOrder = orderRepository.save(order);
        log.info("Order created: id={}, username={}, status={}", savedOrder.getId(), savedOrder.getUsername(), savedOrder.getStatus());

        OrderEvent event = OrderEvent.builder()
                .orderId(savedOrder.getId())
                .customerId(savedOrder.getCustomerId())
                .username(savedOrder.getUsername())
                .items(savedOrder.getItems().stream()
                        .map(item -> OrderEvent.OrderEventItem.builder()
                                .productId(item.getProductId())
                                .quantity(item.getQuantity())
                                .build())
                        .toList())
                .timestamp(savedOrder.getCreatedAt())
                .build();

        orderEventPublisher.publishOrderCreated(event);

        return mapToResponse(savedOrder);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
        return mapToResponse(order);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByUsername(String username) {
        return orderRepository.findByUsername(username).stream()
                .map(this::mapToResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomerId(Long customerId) {
        return orderRepository.findByCustomerId(customerId).stream()
                .map(this::mapToResponse)
                .toList();
    }

    @Transactional
    public void handleStockReserved(StockEvent event) {
        log.info("Stock reserved for order: {}", event.getOrderId());
        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new OrderNotFoundException(event.getOrderId()));
        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Order {} was cancelled, ignoring stock-reserved", event.getOrderId());
            return;
        }
        order.setStatus(OrderStatus.CONFIRMED);
        orderRepository.save(order);
        log.info("Order {} confirmed", event.getOrderId());
    }

    @Transactional
    public void handleStockRejected(StockEvent event) {
        log.info("Stock rejected for order: {}", event.getOrderId());
        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow(() -> new OrderNotFoundException(event.getOrderId()));
        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.warn("Order {} was already cancelled, ignoring stock-rejected", event.getOrderId());
            return;
        }
        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
        log.info("Order {} cancelled", event.getOrderId());

        OrderEvent cancelEvent = OrderEvent.builder()
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .username(order.getUsername())
                .items(order.getItems().stream()
                        .map(item -> OrderEvent.OrderEventItem.builder()
                                .productId(item.getProductId())
                                .quantity(item.getQuantity())
                                .build())
                        .toList())
                .timestamp(order.getUpdatedAt())
                .build();
        orderEventPublisher.publishOrderCancelled(cancelEvent);
    }

    @Transactional
    public OrderResponse cancelOrder(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new OrderNotFoundException(id));
        if (order.getStatus() == OrderStatus.CANCELLED) {
            throw new IllegalOrderStateException("Order " + id + " is already cancelled");
        }
        int updated = orderRepository.markStatusIfActive(id, OrderStatus.CANCELLED, LocalDateTime.now());
        if (updated == 0) {
            throw new IllegalOrderStateException("Order " + id + " is already cancelled");
        }
        Order savedOrder = orderRepository.findById(id).orElseThrow();

        OrderEvent cancelEvent = OrderEvent.builder()
                .orderId(savedOrder.getId())
                .customerId(savedOrder.getCustomerId())
                .username(savedOrder.getUsername())
                .items(savedOrder.getItems().stream()
                        .map(item -> OrderEvent.OrderEventItem.builder()
                                .productId(item.getProductId())
                                .quantity(item.getQuantity())
                                .build())
                        .toList())
                .timestamp(savedOrder.getUpdatedAt())
                .build();
        orderEventPublisher.publishOrderCancelled(cancelEvent);

        return mapToResponse(savedOrder);
    }

    private OrderResponse mapToResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .customerId(order.getCustomerId())
                .username(order.getUsername())
                .status(order.getStatus())
                .total(order.getTotal())
                .items(order.getItems().stream()
                        .map(item -> OrderResponse.OrderItemResponse.builder()
                                .id(item.getId())
                                .productId(item.getProductId())
                                .quantity(item.getQuantity())
                                .price(item.getPrice())
                                .build())
                        .toList())
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .build();
    }
}
