package com.qifa.myhttpclient.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.qifa.myhttpclient.client.ApiClient;
import com.qifa.myhttpclient.exceptions.*;
import com.qifa.myhttpclient.ratelimit.RateLimiterRegistry;
import com.qifa.myhttpclient.ratelimit.TokenBucket;
import com.qifa.myhttpclient.request.BaseRequest;
import com.qifa.myhttpclient.response.BaseResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.*;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * DefaultApi 基类：负责执行 BaseRequest，并返回 BaseResponse 的子类实例。
 * - 在发送请求前处理限流（按 request 提供的 key/capacity/refill 配置）
 * - 使用 ApiClient 提供的 WebClient 发起请求
 * - 将响应 body 反序列化为指定的 responseType
 */
public abstract class DefaultApi {
    private static final Logger logger = LoggerFactory.getLogger(DefaultApi.class);

    protected final ApiClient apiClient;
    protected final ObjectMapper mapper;

    protected DefaultApi(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.mapper = apiClient.getMapper();
    }

    /**
     * 执行异步请求
     * @param req 请求对象（包含路径、方法、体、限流配置等）
     * @param responseType 响应类型
     */
    protected <R extends BaseResponse> Mono<R> execute(BaseRequest<R> req, Class<R> responseType) {
        // 1. 参数校验
        try {
            req.validate();
        } catch (IllegalArgumentException iae) {
            return Mono.error(iae);
        }

        // 2. 限流：若 request 指定了限流，则从 registry 获取或创建令牌桶并尝试获取令牌
        String rlKey = req.getRateLimitKey();
        if (rlKey != null && !rlKey.isBlank() && req.getRateLimitCapacity() > 0) {
            TokenBucket bucket = RateLimiterRegistry.getOrCreate(rlKey, req.getRateLimitCapacity(), req.getRateLimitRefillPerMinute());
            boolean ok = bucket.tryConsumeWithTimeout(1, req.getRateAcquireTimeoutMillis());
            if (!ok) {
                return Mono.error(new RateLimitExceededException("Rate limit exceeded for key: " + rlKey));
            }
        }

        // 3. 构建 URI（包含 query 参数）
        String rawPath = req.getPath();
        // 如果 path 是完整 URL，直接使用；否则使用 ApiClient 的 baseUrl + path
        URI uri = apiClient.buildUri(rawPath, req.getQueryParams());

        // 4. 构建请求
        WebClient.RequestBodySpec spec = apiClient.getWebClient()
                .method(req.getMethod())
                .uri(uri)
                .accept(MediaType.APPLICATION_JSON);

        // headers
        Map<String, String> headers = req.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            headers.forEach(spec::header);
        }
        // correlation id（若未注入，ApiClient 的 filter 会生成）
        Optional<String> maybeCid = Optional.empty();

        WebClient.RequestHeadersSpec<?> finalSpec;
        if (req.getBody() != null) {
            finalSpec = spec.contentType(MediaType.APPLICATION_JSON).bodyValue(req.getBody());
        } else {
            finalSpec = spec;
        }

        // 5. 发送请求并处理响应
        Mono<R> mono = finalSpec
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), resp ->
                        resp.bodyToMono(String.class).defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new ClientException(resp.rawStatusCode(), truncate(body))))
                )
                .onStatus(status -> status.is5xxServerError(), resp ->
                        resp.bodyToMono(String.class).defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new ServerException(resp.rawStatusCode(), truncate(body))))
                )
                .bodyToMono(String.class) // 先拿到原始字符串，再转为目标类型，便于 BaseResponse 的通用字段处理
                .flatMap(bodyStr -> {
                    try {
                        if (responseType == String.class) {
                            // 直接返回 string 包装（前提是 R 能接受 string，或你可定义特例）
                            R inst = responseType.cast(bodyStr);
                            return Mono.just(inst);
                        }
                        R r = mapper.readValue(bodyStr, responseType);
                        return Mono.just(r);
                    } catch (Exception ex) {
                        logger.debug("Failed to deserialize response body for {} {}: {}", req.getMethod(), rawPath, ex.toString());
                        return Mono.error(new NetworkException("Failed to parse response", ex));
                    }
                })
                .doOnError(e -> logger.debug("HTTP call failed: {} {} -> {}", req.getMethod(), rawPath, e.toString()));

        // 6. apply timeout (request-specific)
        if (req.getTimeoutMillis() > 0) {
            mono = mono.timeout(Duration.ofMillis(req.getTimeoutMillis()))
                    .onErrorMap(throwable -> new NetworkException("Timeout or network error", throwable));
        }

        return mono;
    }

    /**
     * 阻塞（同步）执行，使用 request 中的 timeout 作为阻塞最长时间
     */
    protected <R extends BaseResponse> R executeBlocking(BaseRequest<R> req, Class<R> responseType) {
        Duration to = Duration.ofMillis(req.getTimeoutMillis() > 0 ? req.getTimeoutMillis() : apiClient.getDefaultTimeout().toMillis());
        try {
            return execute(req, responseType).block(to);
        } catch (Exception e) {
            // 若是 WebClientResponseException 已被映射为 Client/ServerException，直接抛出
            throw e;
        }
    }

    private static String truncate(String s) {
        if (s == null) return "";
        int max = 2048;
        return s.length() > max ? s.substring(0, max) + "...(truncated)" : s;
    }
}
