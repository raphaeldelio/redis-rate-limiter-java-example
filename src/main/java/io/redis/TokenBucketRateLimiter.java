package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;

public class TokenBucketRateLimiter {

    private final Jedis jedis;
    private final int limit;
    private final double refillRate;

    public TokenBucketRateLimiter(Jedis jedis, int limit, double refillRate) {
        this.jedis = jedis;
        this.limit = limit;
        this.refillRate = refillRate;
    }

    public boolean isAllowed(String clientId) {
        long currentTime = System.currentTimeMillis();
        String keyCount = "rate_limit:" + clientId + ":count";
        String keyLastRefill = "rate_limit:" + clientId + ":lastRefill";
        int bucketCapacity = limit;

        // Retrieve the last refill time and current token count in a transaction
        Transaction transaction = jedis.multi();
        transaction.get(keyLastRefill);
        transaction.get(keyCount);
        List<Object> result = transaction.exec();

        long lastRefillTime = result.get(0) != null ? Long.parseLong((String) result.get(0)) : currentTime;
        int tokenCount = result.get(1) != null ? Integer.parseInt((String) result.get(1)) : bucketCapacity;

        // Calculate time since last refill and tokens to add
        long timeElapsedMs = currentTime - lastRefillTime;
        double timeElapsedSecs = timeElapsedMs / 1000.0;
        int tokensToAdd = (int) (timeElapsedSecs * refillRate);
        int newTokenCount = Math.min(bucketCapacity, tokenCount + tokensToAdd); // Making sure we don't exceed bucket's capacity
        boolean isAllowed = newTokenCount > 0;

        // Update token count and last refill time if tokens were refilled
        transaction = jedis.multi();
        transaction.set(keyCount, String.valueOf(newTokenCount));
        transaction.set(keyLastRefill, String.valueOf(currentTime));
        if (isAllowed) {
            transaction.decr(keyCount);
        }
        transaction.exec();

        return isAllowed;
    }
}