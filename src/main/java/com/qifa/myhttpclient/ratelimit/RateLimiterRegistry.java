package com.qifa.myhttpclient.ratelimit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 管理 TokenBucket 的 Registry。按 key（通常为接口唯一标识）缓存令牌桶。
 */
public class RateLimiterRegistry {
    private static final ConcurrentMap<String, TokenBucket> REG = new ConcurrentHashMap<>();

    /**
     * 获取或创建指定 key 的令牌桶。
     *
     * @param key 唯一 key
     * @param capacity 令牌桶容量
     * @param refillPerMinute 每分钟补给令牌
     * @return TokenBucket
     */
    public static TokenBucket getOrCreate(String key, int capacity, int refillPerMinute) {
        return REG.computeIfAbsent(key, k -> new TokenBucket(capacity, refillPerMinute));
    }
}
