package com.orderflow.notification.service;

import com.orderflow.notification.domain.Notification;
import com.orderflow.notification.domain.NotificationType;
import com.orderflow.notification.repository.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    private NotificationRepository notificationRepository;

    private NotificationService notificationService;

    @BeforeEach
    void setUp() {
        notificationService = new NotificationService(notificationRepository);
    }

    @Test
    void orderCreatedNotificationIsPersisted() {
        notificationService.sendOrderCreatedNotification(5L, 2L);

        Notification saved = captureSaved();
        assertThat(saved.getOrderId()).isEqualTo(5L);
        assertThat(saved.getType()).isEqualTo(NotificationType.ORDER_CREATED);
        assertThat(saved.getRecipient()).isEqualTo("customer-2@orderflow.com");
        assertThat(saved.getMessage()).contains("#5");
    }

    @Test
    void orderCancelledNotificationIsPersisted() {
        notificationService.sendOrderCancelledNotification(5L, 2L);

        Notification saved = captureSaved();
        assertThat(saved.getType()).isEqualTo(NotificationType.ORDER_CANCELLED);
        assertThat(saved.getRecipient()).isEqualTo("customer-2@orderflow.com");
    }

    @Test
    void stockReservedNotificationIsPersisted() {
        notificationService.sendStockReservedNotification(5L);

        Notification saved = captureSaved();
        assertThat(saved.getType()).isEqualTo(NotificationType.ORDER_CONFIRMED);
        assertThat(saved.getRecipient()).isEqualTo("inventory@orderflow.com");
        assertThat(saved.getMessage()).contains("#5");
    }

    private Notification captureSaved() {
        ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
        verify(notificationRepository).save(captor.capture());
        return captor.getValue();
    }
}