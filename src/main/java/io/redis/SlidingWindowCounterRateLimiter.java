package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import java.util.List;
import java.util.Map;

import static redis.clients.jedis.args.ExpiryOption.NX;

public class SlidingWindowCounterRateLimiter {

    private final Jedis jedis;
    private final int limit;
    private final long windowSize;
    private final long subWindowSize;

    public SlidingWindowCounterRateLimiter(Jedis jedis, int limit, long windowSize, long subWindowSize) {
        this.jedis = jedis;
        this.limit = limit;
        this.windowSize = windowSize;
        this.subWindowSize = subWindowSize;
    }

    public boolean isAllowed(String clientId) {
        String key = "rate_limit:" + clientId;
        Map<String, String> subWindowCounts = jedis.hgetAll(key);
        long totalCount = subWindowCounts.values().stream()
                .mapToLong(Long::parseLong)
                .sum();

        boolean isAllowed = totalCount < limit;

        if (isAllowed) {
            // Calculate the current sub-window index based on the time
            long currentTime = System.currentTimeMillis();
            long subWindowSizeMillis = subWindowSize * 1000;
            long currentSubWindow = currentTime / subWindowSizeMillis;

            // Start a transaction to increment the current sub-window count and set TTL
            Transaction transaction = jedis.multi();
            transaction.hincrBy(key, Long.toString(currentSubWindow), 1);
            transaction.hexpire(key, windowSize, NX, String.valueOf(currentSubWindow));
            List<Object> result = transaction.exec();

            if (result == null || result.isEmpty()) {
                throw new IllegalStateException("Empty result from Redis transaction");
            }
        }

        return isAllowed;
    }
}