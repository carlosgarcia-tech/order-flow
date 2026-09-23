package com.orderflow.orders.repository;

import com.orderflow.orders.domain.Order;
import com.orderflow.orders.domain.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    List<Order> findByCustomerId(Long customerId);

    List<Order> findByUsername(String username);

    List<Order> findByStatus(OrderStatus status);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update Order o set o.status = :status, o.updatedAt = :now where o.id = :id and o.status <> :status")
    int markStatusIfActive(@Param("id") Long id, @Param("status") OrderStatus status, @Param("now") LocalDateTime now);
}
