package io.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

public class SlidingWindowLogRateLimiterTest {

    private static final RedisContainer redisContainer = new RedisContainer("redis:latest")
            .withExposedPorts(6379);

    private Jedis jedis;
    private SlidingWindowLogRateLimiter rateLimiter;

    static {
        redisContainer.start();
    }

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
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, 5, 10);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }
    }

    @Test
    public void shouldDenyRequestsOnceLimitIsExceeded() {
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, 5, 60);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request " + i + " should be allowed")
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
        long windowSize = 1L;
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, limit, windowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
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
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, limit, windowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond limit should be denied")
                .isFalse();

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request " + i + " should be allowed")
                    .isTrue();
        }
    }

    @Test
    public void shouldAllowRequestsAgainGraduallyInSlidingWindow() throws InterruptedException {
        int limit = 3;
        long windowSize = 4L;
        String clientId = "client-1";
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, limit, windowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
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

    @Test
    public void testRateLimitDeniedRequestsAreNotCounted() {
        int limit = 3;
        long windowSize = 4L;
        String clientId = "client-1";
        rateLimiter = new SlidingWindowLogRateLimiter(jedis, limit, windowSize);

        for (int i = 1; i <= limit; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request " + i + " should be allowed")
                    .isTrue();
        }

        assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("This request should be denied")
                .isFalse();

        String key = "rate_limit:" + clientId;
        long requestCount = jedis.zcard(key);
        assertThat((long) limit)
                .withFailMessage("The count (" + requestCount + ") should be equal to the limit (" + limit + "), not counting the denied request")
                .isEqualTo(requestCount);
    }
}