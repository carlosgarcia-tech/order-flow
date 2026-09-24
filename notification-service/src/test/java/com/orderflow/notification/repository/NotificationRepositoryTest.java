package com.orderflow.notification.repository;

import com.orderflow.notification.domain.Notification;
import com.orderflow.notification.domain.NotificationStatus;
import com.orderflow.notification.domain.NotificationType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class NotificationRepositoryTest {

    @Autowired
    private NotificationRepository notificationRepository;

    @Test
    void saveMapsAllFieldsAndSetsCreatedAt() {
        Notification notification = Notification.builder()
                .orderId(7L)
                .type(NotificationType.ORDER_CREATED)
                .recipient("customer-2@orderflow.com")
                .message("Pedido #7 creado. Confirma tu compra.")
                .status(NotificationStatus.SENT)
                .build();
        Notification saved = notificationRepository.saveAndFlush(notification);

        Notification loaded = notificationRepository.findById(saved.getId()).orElseThrow();
        assertThat(loaded.getOrderId()).isEqualTo(7L);
        assertThat(loaded.getType()).isEqualTo(NotificationType.ORDER_CREATED);
        assertThat(loaded.getStatus()).isEqualTo(NotificationStatus.SENT);
        assertThat(loaded.getRecipient()).isEqualTo("customer-2@orderflow.com");
        assertThat(loaded.getCreatedAt()).isNotNull();
    }

    @Test
    void findAllReturnsPersistedNotifications() {
        notificationRepository.save(Notification.builder()
                .orderId(1L)
                .type(NotificationType.ORDER_CREATED)
                .recipient("customer-2@orderflow.com")
                .message("Pedido #1 creado")
                .status(NotificationStatus.SENT)
                .build());

        assertThat(notificationRepository.findAll()).hasSize(1);
    }
}