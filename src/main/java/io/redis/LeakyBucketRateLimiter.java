package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class LeakyBucketRateLimiter {
    private final Jedis jedis;
    private final int bucketCapacity; // Maximum requests the bucket can hold
    private final double leakRate;   // Requests leaked per second

    public LeakyBucketRateLimiter(Jedis jedis, int bucketCapacity, double leakRate) {
        this.jedis = jedis;
        this.bucketCapacity = bucketCapacity;
        this.leakRate = leakRate;
    }

    public boolean isAllowed(String clientId) {
        String keyCount = "rate_limit:" + clientId + ":count";
        String keyLastLeak = "rate_limit:" + clientId + ":lastLeak";

        long currentTime = System.currentTimeMillis();

        // Fetch current state
        Transaction transaction = jedis.multi();
        transaction.get(keyLastLeak);
        transaction.get(keyCount);
        var results = transaction.exec();

        long lastLeakTime = results.get(0) != null ? Long.parseLong((String) results.get(0)) : currentTime;
        int requestCount = results.get(1) != null ? Integer.parseInt((String) results.get(1)) : 0;

        // Leak requests
        long elapsedTimeMs = currentTime - lastLeakTime;
        double elapsedTimeSecs = elapsedTimeMs / 1000.0;
        int requestsToLeak = (int) (elapsedTimeSecs * leakRate);
        requestCount = Math.max(0, requestCount - requestsToLeak);

        // Check if the request is allowed
        boolean isAllowed = requestCount < bucketCapacity;
        if (isAllowed) {
            requestCount++; // Add the new request
        }

        // Update Redis state
        transaction = jedis.multi();
        transaction.set(keyLastLeak, String.valueOf(currentTime));
        transaction.set(keyCount, String.valueOf(requestCount));
        transaction.exec();

        return isAllowed;
    }
}