package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class TokenBucketRateLimiter {
    private final Jedis jedis;
    private final int bucketCapacity; // Maximum tokens the bucket can hold
    private final double refillRate; // Tokens refilled per second

    public TokenBucketRateLimiter(Jedis jedis, int bucketCapacity, double refillRate) {
        this.jedis = jedis;
        this.bucketCapacity = bucketCapacity;
        this.refillRate = refillRate;
    }

    public boolean isAllowed(String clientId) {
        String keyCount = "rate_limit:" + clientId + ":count";
        String keyLastRefill = "rate_limit:" + clientId + ":lastRefill";

        long currentTime = System.currentTimeMillis();

        // Fetch current state
        Transaction transaction = jedis.multi();
        transaction.get(keyLastRefill);
        transaction.get(keyCount);
        var results = transaction.exec();

        long lastRefillTime = results.get(0) != null ? Long.parseLong((String) results.get(0)) : currentTime;
        int tokenCount = results.get(1) != null ? Integer.parseInt((String) results.get(1)) : bucketCapacity;

        // Refill tokens
        long elapsedTimeMs = currentTime - lastRefillTime;
        double elapsedTimeSecs = elapsedTimeMs / 1000.0;
        int tokensToAdd = (int) (elapsedTimeSecs * refillRate);
        tokenCount = Math.min(bucketCapacity, tokenCount + tokensToAdd);

        // Check if the request is allowed
        boolean isAllowed = tokenCount > 0;

        if (isAllowed) {
            tokenCount--; // Consume one token
        }

        // Update Redis state
        transaction = jedis.multi();
        transaction.set(keyLastRefill, String.valueOf(currentTime));
        transaction.set(keyCount, String.valueOf(tokenCount));
        transaction.exec();

        return isAllowed;
    }
}