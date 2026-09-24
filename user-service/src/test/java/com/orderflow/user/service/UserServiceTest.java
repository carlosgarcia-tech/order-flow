package com.orderflow.user.service;

import com.orderflow.user.domain.Role;
import com.orderflow.user.domain.User;
import com.orderflow.user.dto.AuthResponse;
import com.orderflow.user.dto.LoginRequest;
import com.orderflow.user.dto.RegisterRequest;
import com.orderflow.user.exception.UserAlreadyExistsException;
import com.orderflow.user.exception.UserNotFoundException;
import com.orderflow.user.repository.UserRepository;
import com.orderflow.user.security.JwtUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserRepository userRepository;
    @Mock
    private PasswordEncoder passwordEncoder;
    @Mock
    private AuthenticationManager authenticationManager;
    @Mock
    private JwtUtil jwtUtil;

    private UserService userService;

    @BeforeEach
    void setUp() {
        userService = new UserService(userRepository, passwordEncoder, authenticationManager, jwtUtil);
        lenient().when(passwordEncoder.encode(any())).thenReturn("encoded-password");
        lenient().when(jwtUtil.generateToken(any(), any(), any())).thenReturn("test-token");
    }

    @Test
    void registerSavesUserAndReturnsToken() {
        when(userRepository.existsByUsername("alice")).thenReturn(false);
        when(userRepository.existsByEmail("alice@example.com")).thenReturn(false);
        when(userRepository.save(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            user.setId(1L);
            return user;
        });

        RegisterRequest request = RegisterRequest.builder()
                .username("alice")
                .email("alice@example.com")
                .password("secret123")
                .build();

        AuthResponse response = userService.register(request);

        assertThat(response.getToken()).isEqualTo("test-token");
        assertThat(response.getUsername()).isEqualTo("alice");
        assertThat(response.getRole()).isEqualTo("ROLE_USER");
        verify(passwordEncoder).encode("secret123");
    }

    @Test
    void registerRejectsDuplicateUsername() {
        when(userRepository.existsByUsername("alice")).thenReturn(true);

        RegisterRequest request = RegisterRequest.builder()
                .username("alice")
                .email("alice@example.com")
                .password("secret123")
                .build();

        assertThatThrownBy(() -> userService.register(request))
                .isInstanceOf(UserAlreadyExistsException.class)
                .hasMessageContaining("alice");
        verify(userRepository, never()).save(any());
    }

    @Test
    void registerRejectsDuplicateEmail() {
        when(userRepository.existsByUsername("bob")).thenReturn(false);
        when(userRepository.existsByEmail("taken@example.com")).thenReturn(true);

        RegisterRequest request = RegisterRequest.builder()
                .username("bob")
                .email("taken@example.com")
                .password("secret123")
                .build();

        assertThatThrownBy(() -> userService.register(request))
                .isInstanceOf(UserAlreadyExistsException.class)
                .hasMessageContaining("taken@example.com");
        verify(userRepository, never()).save(any());
    }

    @Test
    void loginReturnsTokenForValidCredentials() {
        User user = User.builder()
                .id(5L)
                .username("alice")
                .email("alice@example.com")
                .password("encoded")
                .role(Role.ROLE_USER)
                .enabled(true)
                .build();
        when(userRepository.findByUsername("alice")).thenReturn(Optional.of(user));

        LoginRequest request = LoginRequest.builder().username("alice").password("secret123").build();
        AuthResponse response = userService.login(request);

        assertThat(response.getUsername()).isEqualTo("alice");
        assertThat(response.getRole()).isEqualTo("ROLE_USER");
        verify(authenticationManager).authenticate(any());
    }

    @Test
    void getUserByIdThrowsWhenMissing() {
        when(userRepository.findById(99L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> userService.getUserById(99L))
                .isInstanceOf(UserNotFoundException.class);
    }

    @Test
    void getUserByUsernameMapsEntity() {
        User user = User.builder()
                .id(5L)
                .username("alice")
                .email("alice@example.com")
                .password("encoded")
                .role(Role.ROLE_USER)
                .enabled(true)
                .build();
        when(userRepository.findByUsername("alice")).thenReturn(Optional.of(user));

        var response = userService.getUserByUsername("alice");

        assertThat(response.getId()).isEqualTo(5L);
        assertThat(response.getRole()).isEqualTo(Role.ROLE_USER);
    }
}