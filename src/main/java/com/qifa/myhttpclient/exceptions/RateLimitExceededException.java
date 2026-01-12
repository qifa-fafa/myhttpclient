package com.qifa.myhttpclient.exceptions;

/**
 * 当令牌桶获取超时或没有可用令牌时抛出。
 */
public class RateLimitExceededException extends RuntimeException {
    public RateLimitExceededException(String message) { super(message); }
    public RateLimitExceededException(String message, Throwable cause) { super(message, cause); }
}
