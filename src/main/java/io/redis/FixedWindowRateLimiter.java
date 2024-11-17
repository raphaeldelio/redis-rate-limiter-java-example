package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ExpiryOption;

public class FixedWindowRateLimiter {

    private final Jedis jedis;
    private final int windowSize;
    private final int limit;

    public FixedWindowRateLimiter(Jedis jedis, int windowSize, int limit) {
        this.jedis = jedis;
        this.limit = limit;
        this.windowSize = windowSize;
    }

    public boolean isAllowed(String clientId) {
        String key = "rate_limit:" + clientId;
        String currentCountStr = jedis.get(key);
        int currentCount = currentCountStr != null ? Integer.parseInt(currentCountStr) : 0;

        boolean isAllowed = currentCount < limit;

        if (isAllowed) {
            Transaction transaction = jedis.multi();
            transaction.incr(key);
            transaction.expire(key, windowSize, ExpiryOption.NX);
            transaction.exec();
        }
    
        return isAllowed;
    }
}