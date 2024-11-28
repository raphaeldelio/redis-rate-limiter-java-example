package io.redis;

import com.redis.testcontainers.RedisContainer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

public class LeakyBucketRateLimiterTest {

    private static final RedisContainer redisContainer = new RedisContainer("redis:latest")
            .withExposedPorts(6379);

    static {
        redisContainer.start();
    }

    private Jedis jedis;
    private LeakyBucketRateLimiter rateLimiter;

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
    public void shouldAllowRequestsWithinBucketCapacity() {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }
    }

    @Test
    public void shouldDenyRequestsOnceBucketIsFull() {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();
    }

    @Test
    public void shouldAllowRequestsAgainAfterLeakage() throws InterruptedException {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 5, 1.0);
        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        Thread.sleep(1000);

        Assertions.assertThat(rateLimiter.isAllowed("client-1"))
                .withFailMessage("Request after leakage should be allowed")
                .isTrue();
    }

    @Test
    public void shouldMaintainIndependentBucketsForMultipleClients() {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 5, 1.0);
        String clientId1 = "client-1";
        String clientId2 = "client-2";

        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId1))
                    .withFailMessage("Client 1 request %d should be allowed", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId1))
                .withFailMessage("Client 1 request beyond bucket capacity should be denied")
                .isFalse();

        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId2))
                    .withFailMessage("Client 2 request %d should be allowed", i)
                    .isTrue();
        }
    }

    @Test
    public void shouldAllowBurstsUpToBucketCapacity() {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 10, 2.0);
        String clientId = "client-1";

        for (int i = 1; i <= 10; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Burst request %d should be allowed up to bucket capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity in burst should be denied")
                .isFalse();
    }

    @Test
    public void shouldLeakRequestsGraduallyAndAllowRequestsOverTime() throws InterruptedException {
        rateLimiter = new LeakyBucketRateLimiter(jedis, 5, 1.0);
        String clientId = "client-1";

        for (int i = 1; i <= 5; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed within bucket capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        Thread.sleep(2000);

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request after partial refill should be allowed")
                .isTrue();

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Second request after partial refill should be allowed")
                .isTrue();

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond available tokens should be denied")
                .isFalse();
    }

    @Test
    public void shouldFillUpToCapacityWithoutOverflow() throws InterruptedException {
        int capacity = 3;
        double refillRatePerSecond = 2.0;
        String clientId = "client-1";
        rateLimiter = new LeakyBucketRateLimiter(jedis, capacity, refillRatePerSecond);

        for (int i = 1; i <= capacity; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed within initial bucket capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();

        Thread.sleep(3000);

        for (int i = 1; i <= capacity; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed as bucket refills up to capacity", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("Request beyond bucket capacity should be denied")
                .isFalse();
    }

    @Test
    public void shouldNotCountDeniedRequests() {
        int capacity = 3;
        double leakRatePerSecond = 1.0;
        String clientId = "client-1";
        rateLimiter = new LeakyBucketRateLimiter(jedis, capacity, leakRatePerSecond);

        for (int i = 1; i <= capacity; i++) {
            Assertions.assertThat(rateLimiter.isAllowed(clientId))
                    .withFailMessage("Request %d should be allowed", i)
                    .isTrue();
        }

        Assertions.assertThat(rateLimiter.isAllowed(clientId))
                .withFailMessage("This request should be denied")
                .isFalse();

        String key = "rate_limit:" + clientId + ":count";
        int updatedRequestCount = Integer.parseInt(jedis.get(key));

        Assertions.assertThat(updatedRequestCount)
                .withFailMessage("The count (%d) should reflect the leaked requests", updatedRequestCount)
                .isEqualTo(capacity);
    }
}