package com.qifa.myhttpclient.request;

import org.springframework.http.HttpMethod;

import java.util.Collections;
import java.util.Map;

/**
 * 请求基类。子类继承并实现抽象方法以提供请求信息。
 *
 * @param <T> 响应类型，必须继承 BaseResponse
 */
public abstract class BaseRequest<T extends com.qifa.myhttpclient.response.BaseResponse> {

    /**
     * 请求路径（相对于 ApiClient 的 baseUrl），例如 "/v1/ping" 或完整 URL。
     */
    public abstract String getPath();

    /**
     * HTTP 方法
     */
    public abstract HttpMethod getMethod();

    /**
     * 请求头（可选）
     */
    public Map<String, String> getHeaders() {
        return Collections.emptyMap();
    }

    /**
     * 请求体（可选），将通过 WebClient 以 JSON 序列化（或直接传入 String）
     */
    public Object getBody() {
        return null;
    }

    /**
     * 查询参数（可选）
     */
    public Map<String, String> getQueryParams() {
        return Collections.emptyMap();
    }

    /**
     * 请求超时（毫秒），默认 20s
     */
    public long getTimeoutMillis() {
        return 20_000L;
    }

    /**
     * 限流 key（如果为 null 表示不使用限流）。建议每个接口使用独立 key，例如 "ThirdPartyApi#getPing"。
     */
    public String getRateLimitKey() {
        return null;
    }

    /**
     * 令牌桶容量（单位：令牌数）。若为 <= 0 则关闭限流（不会创建桶）。
     */
    public int getRateLimitCapacity() {
        return 0;
    }

    /**
     * 每分钟补充令牌数（单位：令牌数）。推荐小于等于 capacity.
     */
    public int getRateLimitRefillPerMinute() {
        return 0;
    }

    /**
     * 获取令牌时等待超时（毫秒）。若为 0 则立即尝试，不阻塞。
     */
    public long getRateAcquireTimeoutMillis() {
        return 0L;
    }

    /**
     * 可由子类覆盖，用于请求前参数校验
     */
    public void validate() throws IllegalArgumentException {
        // default no-op
    }
}
