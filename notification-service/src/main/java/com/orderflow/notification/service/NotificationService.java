package com.orderflow.notification.service;

import com.orderflow.notification.domain.Notification;
import com.orderflow.notification.domain.NotificationStatus;
import com.orderflow.notification.domain.NotificationType;
import com.orderflow.notification.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;

    @Value("${notification.email.enabled:false}")
    private boolean emailEnabled;

    public void sendOrderCreatedNotification(Long orderId, Long customerId) {
        String message = String.format(
                "Pedido #%d creado exitosamente por el cliente %d", orderId, customerId);

        log.info("NOTIFICATION [ORDER_CREATED]: {}", message);
        saveNotification(orderId, NotificationType.ORDER_CREATED,
                "customer-" + customerId + "@orderflow.com", message);
    }

    public void sendOrderCancelledNotification(Long orderId, Long customerId) {
        String message = String.format(
                "Pedido #%d del cliente %d ha sido cancelado", orderId, customerId);

        log.info("NOTIFICATION [ORDER_CANCELLED]: {}", message);
        saveNotification(orderId, NotificationType.ORDER_CANCELLED,
                "customer-" + customerId + "@orderflow.com", message);
    }

    public void sendStockReservedNotification(Long orderId) {
        String message = String.format(
                "Pedido #%d confirmado - stock reservado exitosamente", orderId);

        log.info("NOTIFICATION [ORDER_CONFIRMED]: {}", message);
        saveNotification(orderId, NotificationType.ORDER_CONFIRMED,
                "inventory@orderflow.com", message);
    }

    private void saveNotification(Long orderId, NotificationType type,
                                   String recipient, String message) {
        Notification notification = Notification.builder()
                .orderId(orderId)
                .type(type)
                .recipient(recipient)
                .message(message)
                .status(NotificationStatus.SENT)
                .sentAt(LocalDateTime.now())
                .build();
        notificationRepository.save(notification);
    }
}
