package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;

public class SlidingWindowLogRateLimiter {

    private final Jedis jedis;
    private final int limit;
    private final long windowSize;

    public SlidingWindowLogRateLimiter(Jedis jedis, int limit, long windowSize) {
        this.jedis = jedis;
        this.limit = limit;
        this.windowSize = windowSize;
    }

    public boolean isAllowed(String clientId) {
        String key = "rate_limit:" + clientId;

        long currentTime = System.currentTimeMillis();
        long windowStartTime = currentTime - windowSize * 1000;

        Transaction transaction = jedis.multi();
        transaction.zremrangeByScore(key, 0, windowStartTime);
        transaction.zcard(key);
        List<Object> result = transaction.exec();

        if (result.isEmpty()) {
            throw new IllegalStateException("Empty result from Redis pipeline");
        }

        long requestCount = (Long) result.get(1);
        boolean isAllowed = requestCount < limit;

        if (isAllowed) {
            String uniqueMember = currentTime + "-" + UUID.randomUUID();
            transaction = jedis.multi();
            transaction.zadd(key, currentTime, uniqueMember);
            transaction.expire(key, (int) windowSize);
            transaction.exec();
        }

        return isAllowed;
    }

    public boolean isAllowedHashAlternative(String clientId) {
        String key = "rate_limit:" + clientId;
        String fieldKey = UUID.randomUUID().toString();

        long requestCount = jedis.hlen(key);
        boolean isAllowed = requestCount < limit;

        if (isAllowed) {
            Transaction transaction = jedis.multi();
            transaction.hset(key, fieldKey, "");
            transaction.hexpire(key, windowSize, fieldKey);
            transaction.exec();
        }

        return isAllowed;
    }
}