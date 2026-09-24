package com.orderflow.orders.repository;

import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class OrderRepositoryTest {

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void saveAndFindByUsername() {
        Order order = Order.builder()
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("199.98"))
                .build();
        orderRepository.saveAndFlush(order);

        assertThat(orderRepository.findByUsername("alice")).hasSize(1);
        assertThat(orderRepository.findByCustomerId(7L)).hasSize(1);
    }

    @Test
    void markStatusIfActiveTransitionsOnlyOnce() {
        Order order = Order.builder()
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.PENDING)
                .total(new BigDecimal("199.98"))
                .build();
        orderRepository.saveAndFlush(order);

        int first = orderRepository.markStatusIfActive(order.getId(), OrderStatus.CANCELLED, LocalDateTime.now());
        Order reloaded = orderRepository.findById(order.getId()).orElseThrow();

        assertThat(first).isEqualTo(1);
        assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.CANCELLED);

        int second = orderRepository.markStatusIfActive(order.getId(), OrderStatus.CANCELLED, LocalDateTime.now());
        assertThat(second).isZero();
    }

    @Test
    void markStatusIfActiveCanCancelConfirmedOrder() {
        Order order = Order.builder()
                .customerId(7L)
                .username("alice")
                .status(OrderStatus.CONFIRMED)
                .total(new BigDecimal("199.98"))
                .build();
        orderRepository.saveAndFlush(order);

        int updated = orderRepository.markStatusIfActive(order.getId(), OrderStatus.CANCELLED, LocalDateTime.now());
        Order reloaded = orderRepository.findById(order.getId()).orElseThrow();

        assertThat(updated).isEqualTo(1);
        assertThat(reloaded.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }
}