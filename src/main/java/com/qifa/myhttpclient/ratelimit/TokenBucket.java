package com.qifa.myhttpclient.ratelimit;

import java.util.concurrent.TimeUnit;

/**
 * 简单令牌桶实现（按分钟补给）。
 * 线程安全，使用 synchronized 实现。
 */
public class TokenBucket {
    private final int capacity;
    private final int refillPerMinute;
    private double tokens;
    private long lastRefillTimeMillis;

    /**
     * @param capacity 最大令牌数
     * @param refillPerMinute 每分钟补充的令牌数
     */
    public TokenBucket(int capacity, int refillPerMinute) {
        this.capacity = Math.max(1, capacity);
        this.refillPerMinute = Math.max(0, refillPerMinute);
        this.tokens = this.capacity;
        this.lastRefillTimeMillis = System.currentTimeMillis();
    }

    /**
     * 进行补给（基于分钟粒度，部分令牌按比例增加）
     */
    private synchronized void refill() {
        long now = System.currentTimeMillis();
        long elapsedMillis = now - lastRefillTimeMillis;
        if (elapsedMillis <= 0 || refillPerMinute <= 0) {
            lastRefillTimeMillis = now;
            return;
        }
        double minutes = elapsedMillis / 60000.0;
        double add = minutes * refillPerMinute;
        if (add > 0) {
            tokens = Math.min(capacity, tokens + add);
            lastRefillTimeMillis = now;
        }
    }

    /**
     * 尝试立即消费 1 个令牌（非阻塞）。
     * @return 成功消费返回 true，否则 false
     */
    public boolean tryConsume() {
        return tryConsume(1);
    }

    /**
     * 尝试立即消费 n 个令牌（非阻塞）。
     */
    public synchronized boolean tryConsume(int n) {
        refill();
        if (tokens >= n) {
            tokens -= n;
            return true;
        }
        return false;
    }

    /**
     * 尝试带超时获取 n 个令牌。
     * @param n 令牌数
     * @param timeoutMillis 超时毫秒
     * @return 成功返回 true，超时或中断返回 false
     */
    public boolean tryConsumeWithTimeout(int n, long timeoutMillis) {
        final long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            synchronized (this) {
                refill();
                if (tokens >= n) {
                    tokens -= n;
                    return true;
                }
            }
            if (timeoutMillis <= 0 || System.currentTimeMillis() >= deadline) {
                return false;
            }
            // sleep 一小段时间后重试（避免 busy spin）
            long sleepMs = Math.min(100, Math.max(10, timeoutMillis / 10));
            long remain = deadline - System.currentTimeMillis();
            if (remain <= 0) return false;
            try {
                TimeUnit.MILLISECONDS.sleep(Math.min(sleepMs, remain));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    public int getCapacity() { return capacity; }
    public int getRefillPerMinute() { return refillPerMinute; }
}
