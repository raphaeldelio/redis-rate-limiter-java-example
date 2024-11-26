package io.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

public class SlidingWindowCounterRateLimiterTest {

    private static final RedisContainer redisContainer = new RedisContainer("redis:latest")
            .withExposedPorts(6379)
            .withReuse(true);

    static {
        redisContainer.start();
    }

    private Jedis jedis;
    private SlidingWindowCounterRateLimiter rateLimiter;

    @BeforeEach
    public void setup() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    public void tearDown() {
        jedis.close();
    }

    @Test
    public void shouldAllowRequestsWithinLimit() {
        rateLimiter = new SlidingWindowCounterRateLimiter(jedis, 5, 10, 1);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    public void shouldDenyRequestsOnceLimitIsExceeded() {
        rateLimiter = new SlidingWindowCounterRateLimiter(jedis, 5, 60, 1);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();
    }

    @Test
    public void shouldAllowRequestsAgainAfterSlidingWindowResets() throws InterruptedException {
        int limit = 5;
        String clientId = "client-1";
        long windowSize = 2L;
        long subWindowSize = 1L;

        rateLimiter = new SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep((windowSize + 1) * 1000);

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request after window reset should be allowed")
                .isTrue();
    }

    @Test
    public void shouldHandleMultipleClientsIndependently() {
        int limit = 5;
        String clientId1 = "client-1";
        String clientId2 = "client-2";
        long windowSize = 10L;
        long subWindowSize = 1L;

        rateLimiter = new SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request %d should be allowed", i)
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond limit should be denied")
                .isFalse();

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    public void shouldAllowRequestsAgainGraduallyInSlidingWindow() throws InterruptedException {
        int limit = 3;
        long windowSize = 4L;
        long subWindowSize = 1L;
        String clientId = "client-1";

        rateLimiter = new SlidingWindowCounterRateLimiter(jedis, limit, windowSize, subWindowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
            Thread.sleep(1000);
        }

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond limit should be denied")
                .isFalse();

        Thread.sleep(2000);

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request should be allowed in a sliding window")
                .isTrue();
    }
}