package io.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        long currentTime = System.currentTimeMillis();
        long subWindowSizeMillis = subWindowSize * 1000;
        long numSubWindows = (windowSize * 1000) / subWindowSizeMillis;

        String key = "rate_limit:" + clientId;

        // Calculate the current sub-window index based on the time
        long currentSubWindow = currentTime / subWindowSizeMillis;

        // Start a transaction to increment the current sub-window count and set TTL
        Transaction transaction = jedis.multi();
        transaction.hincrBy(key, Long.toString(currentSubWindow), 1);
        String[] fields = new String[1000];
        Arrays.fill(fields, Long.toString(currentSubWindow)); // Fill the array with the currentSubWindow value
        transaction.hexpire(key, windowSize, fields);

        // Calculate the total counts for all sub-windows within the sliding window
        List<Long> subWindowsToCheck = new ArrayList<>();
        for (long subWindow = currentSubWindow; subWindow >= (currentSubWindow - numSubWindows + 1); subWindow--) {
            subWindowsToCheck.add(subWindow);
        }
        subWindowsToCheck.forEach(subWindow -> transaction.hget(key, Long.toString(subWindow)));

        List<Object> result = transaction.exec();

        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("Empty result from Redis transaction");
        }

        long totalCount = result.stream()
                .skip(2)
                .map(it -> it == null ? 0 : Long.parseLong((String) it))
                .reduce(0L, Long::sum);

        // Check if the total count is within the limit
        return totalCount <= limit;
    }
}