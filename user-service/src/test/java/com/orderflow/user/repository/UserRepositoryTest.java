package com.orderflow.user.repository;

import com.orderflow.user.domain.Role;
import com.orderflow.user.domain.User;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class UserRepositoryTest {

    @Autowired
    private UserRepository userRepository;

    @Test
    void findByUsernameReturnsSavedUser() {
        User user = User.builder()
                .username("carol")
                .email("carol@example.com")
                .password("encoded")
                .role(Role.ROLE_USER)
                .enabled(true)
                .build();
        userRepository.saveAndFlush(user);

        assertThat(userRepository.findByUsername("carol")).isPresent();
    }

    @Test
    void existsByUsernameAndEmail() {
        User user = User.builder()
                .username("carol")
                .email("carol@example.com")
                .password("encoded")
                .role(Role.ROLE_USER)
                .enabled(true)
                .build();
        userRepository.saveAndFlush(user);

        assertThat(userRepository.existsByUsername("carol")).isTrue();
        assertThat(userRepository.existsByUsername("nobody")).isFalse();
        assertThat(userRepository.existsByEmail("carol@example.com")).isTrue();
        assertThat(userRepository.existsByEmail("nobody@example.com")).isFalse();
    }
}