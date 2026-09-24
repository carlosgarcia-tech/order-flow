package com.orderflow.user.controller;

import com.orderflow.user.domain.User;
import com.orderflow.user.repository.UserRepository;
import com.orderflow.user.security.JwtUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
class UserControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private JwtUtil jwtUtil;

    private static final String REGISTER = "{\"username\":\"alice\",\"email\":\"alice@example.com\",\"password\":\"secret123\"}";

    @Test
    void registerCreatesUser() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                        .contentType("application/json")
                        .content(REGISTER))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.username").value("alice"))
                .andExpect(jsonPath("$.role").value("ROLE_USER"))
                .andExpect(jsonPath("$.token").isNotEmpty());
    }

    @Test
    void registerRejectsDuplicateUsername() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                        .contentType("application/json")
                        .content(REGISTER))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/auth/register")
                        .contentType("application/json")
                        .content(REGISTER))
                .andExpect(status().isConflict());
    }

    @Test
    void registerRejectsInvalidPayload() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                        .contentType("application/json")
                        .content("{\"username\":\"x\",\"email\":\"not-an-email\",\"password\":\"1\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void registerRejectsMalformedJson() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                        .contentType("application/json")
                        .content("{not-json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void loginReturnsToken() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));

        mockMvc.perform(post("/api/auth/login")
                        .contentType("application/json")
                        .content("{\"username\":\"alice\",\"password\":\"secret123\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").isNotEmpty());
    }

    @Test
    void loginRejectsWrongPassword() throws Exception {
        mockMvc.perform(post("/api/auth/login")
                        .contentType("application/json")
                        .content("{\"username\":\"alice\",\"password\":\"wrong\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getProfileRequiresAuthentication() throws Exception {
        mockMvc.perform(get("/api/users/me"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getCurrentUserReturnsOwnProfile() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));
        User alice = userRepository.findByUsername("alice").orElseThrow();

        mockMvc.perform(get("/api/users/me")
                        .header("Authorization", bearer(alice.getId())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("alice"));
    }

    @Test
    void getUserByIdReturnsOwnProfile() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));
        User alice = userRepository.findByUsername("alice").orElseThrow();

        mockMvc.perform(get("/api/users/" + alice.getId())
                        .header("Authorization", bearer(alice.getId())))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("alice"));
    }

    @Test
    void getUserByIdWithNonNumericIdIsBadRequest() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));
        User alice = userRepository.findByUsername("alice").orElseThrow();

        mockMvc.perform(get("/api/users/abc")
                        .header("Authorization", bearer(alice.getId())))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getUserByIdWithMissingIdIsNotFound() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));
        User alice = userRepository.findByUsername("alice").orElseThrow();

        mockMvc.perform(get("/api/users/99999")
                        .header("Authorization", bearer(alice.getId())))
                .andExpect(status().isNotFound());
    }

    @Test
    void listUsersRequiresAdminRole() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));
        User alice = userRepository.findByUsername("alice").orElseThrow();

        mockMvc.perform(get("/api/users")
                        .header("Authorization", bearer(alice.getId())))
                .andExpect(status().isForbidden());
    }

    @Test
    void listUsersAllowedForAdmin() throws Exception {
        mockMvc.perform(post("/api/auth/register")
                .contentType("application/json")
                .content(REGISTER));

        mockMvc.perform(get("/api/users")
                        .header("Authorization", "Bearer " + jwtUtil.generateToken("boss", "ROLE_ADMIN", 999L)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }

    private String bearer(Long userId) {
        return "Bearer " + jwtUtil.generateToken("alice", "ROLE_USER", userId);
    }
}