package io.redis;

import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.*;
import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TokenBucketRateLimiterTest {

    private static RedisContainer redisContainer;
    private Jedis jedis;
    private TokenBucketRateLimiter rateLimiter;

    @BeforeAll
    static void startContainer() {
        redisContainer = new RedisContainer("redis:latest");
        redisContainer.withExposedPorts(6379).start();
    }

    @AfterAll
    static void stopContainer() {
        redisContainer.stop();
    }

    @BeforeEach
    void setup() {
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.flushAll();
    }

    @AfterEach
    void tearDown() {
        jedis.close();
    }

    @Test
    void shouldAllowRequestsWithinBucketCapacity() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request %d should be allowed within bucket capacity", i)
                .isTrue();
        }
    }

    @Test
    void shouldDenyRequestsOnceBucketIsEmpty() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request %d should be allowed within bucket capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse();
    }

    @Test
    void shouldAllowRequestsAgainAfterTokensAreRefilled() throws InterruptedException {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request %d should be allowed within bucket capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse();

        TimeUnit.SECONDS.sleep(1);

        assertThat(rateLimiter.isAllowed("client-1"))
            .withFailMessage("Request after token refill should be allowed")
            .isTrue();
    }

    @Test
    void shouldHandleMultipleClientsIndependently() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);

        String clientId1 = "client-1";
        String clientId2 = "client-2";

        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request %d should be allowed", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId1))
            .withFailMessage("Client 1 request beyond bucket capacity should be denied")
            .isFalse();

        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed(clientId2))
                .withFailMessage("Client 2 request %d should be allowed", i)
                .isTrue();
        }
    }

    @Test
    void shouldAllowBurstsUpToBucketCapacity() {
        rateLimiter = new TokenBucketRateLimiter(jedis, 10, 2.0);

        String clientId = "client-1";

        for (int i = 1; i <= 10; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Burst request %d should be allowed up to bucket capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity in burst should be denied")
            .isFalse();
    }

    @Test
    void shouldRefillTokensGraduallyAndAllowRequestsOverTime() throws InterruptedException {
        rateLimiter = new TokenBucketRateLimiter(jedis, 5, 1.0);
        String clientId = "client-1";

        for (int i = 1; i <= 5; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request %d should be allowed within bucket capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse();

        TimeUnit.SECONDS.sleep(2);

        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request after partial refill should be allowed")
            .isTrue();
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Second request after partial refill should be allowed")
            .isTrue();
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond available tokens should be denied")
            .isFalse();
    }

    @Test
    void shouldRefillTokensUpToCapacityWithoutExceedingIt() throws InterruptedException {
        int capacity = 3;
        double refillRate = 2.0;
        String clientId = "client-1";
        rateLimiter = new TokenBucketRateLimiter(jedis, capacity, refillRate);

        for (int i = 1; i <= capacity; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request %d should be allowed within initial bucket capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse();

        TimeUnit.SECONDS.sleep(3);

        for (int i = 1; i <= capacity; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request %d should be allowed as bucket refills up to capacity", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("Request beyond bucket capacity should be denied")
            .isFalse();
    }

    @Test
    void testRateLimitDeniedRequestsAreNotCounted() {
        int capacity = 3;
        double refillRate = 0.5;
        String clientId = "client-1";
        rateLimiter = new TokenBucketRateLimiter(jedis, capacity, refillRate);

        for (int i = 1; i <= capacity; i++) {
            assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request %d should be allowed", i)
                .isTrue();
        }
        assertThat(rateLimiter.isAllowed(clientId))
            .withFailMessage("This request should be denied")
            .isFalse();

        String key = "rate_limit:" + clientId + ":count";
        int requestCount = Integer.parseInt(jedis.get(key));
        assertThat(requestCount)
            .withFailMessage("The count should match remaining tokens and not include denied requests")
            .isEqualTo(0);
    }
}