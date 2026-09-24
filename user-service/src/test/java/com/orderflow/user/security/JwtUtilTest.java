package com.orderflow.user.security;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class JwtUtilTest {

    private static final String SECRET = "mySecretKeyThatIsAtLeast32BytesLongForHS256Algorithm!";

    private JwtUtil jwtUtil;

    @BeforeEach
    void setUp() {
        jwtUtil = new JwtUtil();
        ReflectionTestUtils.setField(jwtUtil, "secret", SECRET);
        ReflectionTestUtils.setField(jwtUtil, "expiration", 3_600_000L);
    }

    @Test
    void generatedTokenIsValidAndCarriesClaims() {
        String token = jwtUtil.generateToken("alice", "ROLE_USER", 7L);

        assertThat(jwtUtil.validateToken(token)).isTrue();
        assertThat(jwtUtil.getUsernameFromToken(token)).isEqualTo("alice");
        assertThat(jwtUtil.getRoleFromToken(token)).isEqualTo("ROLE_USER");
        assertThat(jwtUtil.getUserIdFromToken(token)).isEqualTo(7L);
    }

    @Test
    void tamperedSignatureIsRejected() {
        String token = jwtUtil.generateToken("alice", "ROLE_USER", 7L);
        String tampered = token.substring(0, token.length() - 1) + "X";

        assertThat(jwtUtil.validateToken(tampered)).isFalse();
    }

    @Test
    void nonCanonicalSignatureIsRejected() {
        String token = jwtUtil.generateToken("alice", "ROLE_USER", 7L);
        String nonCanonical = token + "A";

        assertThat(jwtUtil.validateToken(nonCanonical)).isFalse();
    }

    @Test
    void expiredTokenIsRejected() {
        SecretKey key = Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8));
        String token = Jwts.builder()
                .subject("alice")
                .claim("role", "ROLE_USER")
                .claim("userId", 7L)
                .issuedAt(new Date(System.currentTimeMillis() - 10_000))
                .expiration(new Date(System.currentTimeMillis() - 5_000))
                .signWith(key)
                .compact();

        assertThat(jwtUtil.validateToken(token)).isFalse();
    }

    @Test
    void algorithmNoneTokenIsRejected() {
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url("{\"sub\":\"alice\",\"role\":\"ROLE_USER\",\"userId\":7,\"exp\":4102444800}");

        assertThat(jwtUtil.validateToken(header + "." + payload + ".")).isFalse();
    }

    @Test
    void tokenWithExtraSegmentIsRejected() {
        String token = jwtUtil.generateToken("alice", "ROLE_USER", 7L);

        assertThat(jwtUtil.validateToken(token + ".extra")).isFalse();
    }

    @Test
    void malformedTokenIsRejected() {
        assertThat(jwtUtil.validateToken("not-a-jwt")).isFalse();
    }

    private static String base64Url(String value) {
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}