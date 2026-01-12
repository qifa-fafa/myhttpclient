package com.qifa.myhttpclient.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qifa.myhttpclient.client.ApiClient;
import com.qifa.myhttpclient.CorrelationIdUtil;
import com.qifa.myhttpclient.exceptions.ClientException;
import com.qifa.myhttpclient.exceptions.ServerException;
import com.qifa.myhttpclient.exceptions.NetworkException;
import io.github.resilience4j.circuitbreaker.*;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.retry.*;
import io.github.resilience4j.reactor.retry.RetryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.*;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * DefaultApi is a base class for concrete APIs.
 * - Provides invokeAsync(...) returning Mono<T>
 * - Provides invoke(...) convenience blocking method (uses ApiClient.defaultTimeout)
 *
 * Subclasses can:
 *  - call protected invoke/invokeAsync directly
 *  - override behavior if needed (e.g. response mapping)
 */
public abstract class DefaultApi {
    private static final Logger logger = LoggerFactory.getLogger(DefaultApi.class);

    protected final ApiClient apiClient;
    protected final ObjectMapper mapper;

    // semaphores keyed by maxConcurrentRequests to allow different calls to opt into different concurrency limits
    private final ConcurrentHashMap<Integer, Semaphore> semaphores = new ConcurrentHashMap<>();

    // registries for resilience4j artifacts (cached per name)
    private final ConcurrentMap<String, io.github.resilience4j.retry.Retry> resilienceRetries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, io.github.resilience4j.circuitbreaker.CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();

    protected DefaultApi(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.mapper = apiClient.getMapper();
    }

    /**
     * Call options controlling retry and simple concurrency-based rate limiting.
     * By default both retry and rate limiting are disabled.
     *
     * New resilience fields (enableResilienceRetry/enableCircuitBreaker) allow using resilience4j
     * with named Retry/CircuitBreaker instances. If not enabled, the previous simple reactive retry/backoff is used.
     */
    public static final class CallOptions {
        // legacy/simple retry (reactor.util.retry)
        private final boolean enableRetry;
        private final int maxRetries;
        private final long retryBackoffMillis;

        // simple concurrency-based rate limiting
        private final boolean enableRateLimit;
        private final int maxConcurrentRequests; // treated as semaphore permits
        private final long rateAcquireTimeoutMillis; // timeout to acquire permit

        // resilience4j options
        private final boolean enableResilienceRetry;
        private final String resilienceRetryName;

        private final boolean enableCircuitBreaker;
        private final String circuitBreakerName;

        private CallOptions(boolean enableRetry, int maxRetries, long retryBackoffMillis,
                            boolean enableRateLimit, int maxConcurrentRequests, long rateAcquireTimeoutMillis,
                            boolean enableResilienceRetry, String resilienceRetryName,
                            boolean enableCircuitBreaker, String circuitBreakerName) {
            this.enableRetry = enableRetry;
            this.maxRetries = maxRetries;
            this.retryBackoffMillis = retryBackoffMillis;
            this.enableRateLimit = enableRateLimit;
            this.maxConcurrentRequests = maxConcurrentRequests;
            this.rateAcquireTimeoutMillis = rateAcquireTimeoutMillis;
            this.enableResilienceRetry = enableResilienceRetry;
            this.resilienceRetryName = resilienceRetryName;
            this.enableCircuitBreaker = enableCircuitBreaker;
            this.circuitBreakerName = circuitBreakerName;
        }

        public static Builder builder() { return new Builder(); }

        public static CallOptions defaults() { return new Builder().build(); }

        public boolean isEnableRetry() { return enableRetry; }
        public int getMaxRetries() { return maxRetries; }
        public long getRetryBackoffMillis() { return retryBackoffMillis; }
        public boolean isEnableRateLimit() { return enableRateLimit; }
        public int getMaxConcurrentRequests() { return maxConcurrentRequests; }
        public long getRateAcquireTimeoutMillis() { return rateAcquireTimeoutMillis; }
        public boolean isEnableResilienceRetry() { return enableResilienceRetry; }
        public String getResilienceRetryName() { return resilienceRetryName; }
        public boolean isEnableCircuitBreaker() { return enableCircuitBreaker; }
        public String getCircuitBreakerName() { return circuitBreakerName; }

        public static final class Builder {
            private boolean enableRetry = false;
            private int maxRetries = 3;
            private long retryBackoffMillis = 500;
            private boolean enableRateLimit = false;
            private int maxConcurrentRequests = 50;
            private long rateAcquireTimeoutMillis = 1000;

            private boolean enableResilienceRetry = false;
            private String resilienceRetryName = "default-retry";

            private boolean enableCircuitBreaker = false;
            private String circuitBreakerName = "default-cb";

            public Builder enableRetry(boolean enable) { this.enableRetry = enable; return this; }
            public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
            public Builder retryBackoffMillis(long ms) { this.retryBackoffMillis = ms; return this; }
            public Builder enableRateLimit(boolean enable) { this.enableRateLimit = enable; return this; }
            public Builder maxConcurrentRequests(int max) { this.maxConcurrentRequests = max; return this; }
            public Builder rateAcquireTimeoutMillis(long ms) { this.rateAcquireTimeoutMillis = ms; return this; }

            public Builder enableResilienceRetry(boolean enable) { this.enableResilienceRetry = enable; return this; }
            public Builder resilienceRetryName(String name) { this.resilienceRetryName = name; return this; }

            public Builder enableCircuitBreaker(boolean enable) { this.enableCircuitBreaker = enable; return this; }
            public Builder circuitBreakerName(String name) { this.circuitBreakerName = name; return this; }

            public CallOptions build() {
                return new CallOptions(enableRetry, maxRetries, retryBackoffMillis,
                        enableRateLimit, maxConcurrentRequests, rateAcquireTimeoutMillis,
                        enableResilienceRetry, resilienceRetryName,
                        enableCircuitBreaker, circuitBreakerName);
            }
        }
    }

    protected <T> Mono<T> invokeAsync(
            HttpMethod method,
            String path,
            Object requestBody,
            Class<T> responseType,
            Map<String, String> headers,
            Optional<String> maybeCorrelationId
    ) {
        return invokeAsync(method, path, requestBody, responseType, headers, maybeCorrelationId, CallOptions.defaults());
    }

    protected <T> Mono<T> invokeAsync(
            HttpMethod method,
            String path,
            Object requestBody,
            Class<T> responseType,
            Map<String, String> headers,
            Optional<String> maybeCorrelationId,
            CallOptions callOptions
    ) {
        WebClient.RequestBodySpec reqSpec = apiClient.getWebClient()
                .method(method)
                .uri(path)
                .accept(MediaType.APPLICATION_JSON);

        // apply headers
        if (headers != null) {
            headers.forEach(reqSpec::header);
        }
        // correlation id from parameter takes precedence
        if (maybeCorrelationId != null && maybeCorrelationId.isPresent()) {
            reqSpec.header(CorrelationIdUtil.HEADER, maybeCorrelationId.get());
        }

        WebClient.RequestHeadersSpec<?> finalSpec;
        if (requestBody != null) {
            finalSpec = reqSpec
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(requestBody);
        } else {
            finalSpec = reqSpec;
        }

        Mono<T> base = finalSpec
                .retrieve()
                .onStatus(status -> status.is4xxClientError(), resp ->
                        resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new ClientException(resp.statusCode().value(), body)))
                )
                .onStatus(status -> status.is5xxServerError(), resp ->
                        resp.bodyToMono(String.class)
                                .defaultIfEmpty("")
                                .flatMap(body -> Mono.error(new ServerException(resp.statusCode().value(), body)))
                )
                .bodyToMono(responseType)
                .doOnError(e -> logger.debug("HTTP call failed: {} {} -> {}", method, path, e.toString()));

        Mono<T> guarded = base;

        // rate limiting (simple concurrency limit using Semaphore)
        if (callOptions != null && callOptions.isEnableRateLimit()) {
            final int permits = Math.max(1, callOptions.getMaxConcurrentRequests());
            final Semaphore sem = semaphores.computeIfAbsent(permits, k -> new Semaphore(k));
            guarded = Mono.defer(() -> {
                boolean acquired;
                try {
                    acquired = sem.tryAcquire(callOptions.getRateAcquireTimeoutMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Mono.error(new NetworkException("Interrupted acquiring rate limit permit", e));
                }
                if (!acquired) {
                    return Mono.error(new NetworkException("Rate limit: timeout acquiring permit"));
                }
                return base.doFinally(sig -> sem.release());
            });
        }

        // resilience4j CircuitBreaker
        if (callOptions != null && callOptions.isEnableCircuitBreaker()) {
            String cbName = callOptions.getCircuitBreakerName() != null ? callOptions.getCircuitBreakerName() : "default-cb";
            io.github.resilience4j.circuitbreaker.CircuitBreaker cb = circuitBreakers.computeIfAbsent(cbName, name -> {
                CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .slidingWindowSize(20)
                        .minimumNumberOfCalls(5)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .build();
                return io.github.resilience4j.circuitbreaker.CircuitBreaker.of(name, config);
            });
            guarded = guarded.transformDeferred(CircuitBreakerOperator.of(cb));
        }

        // resilience4j Retry (reactor)
        if (callOptions != null && callOptions.isEnableResilienceRetry()) {
            String retryName = callOptions.getResilienceRetryName() != null ? callOptions.getResilienceRetryName() : "default-retry";
            io.github.resilience4j.retry.Retry r = resilienceRetries.computeIfAbsent(retryName, name -> {
                RetryConfig cfg = RetryConfig.custom()
                        .maxAttempts(3)
                        .waitDuration(Duration.ofMillis(500))
                        .retryExceptions(NetworkException.class, WebClientRequestException.class)
                        .build();
                return io.github.resilience4j.retry.Retry.of(name, cfg);
            });
            guarded = guarded.transformDeferred(RetryOperator.of(r));
        } else if (callOptions != null && callOptions.isEnableRetry()) {
            // legacy reactor retry/backoff (kept for backward compatibility)
            guarded = guarded.retryWhen(
                    Retry.backoff(callOptions.getMaxRetries(), Duration.ofMillis(callOptions.getRetryBackoffMillis()))
                            .filter(throwable -> (throwable instanceof NetworkException)
                                    || (throwable instanceof WebClientRequestException)
                                    || (throwable.getCause() instanceof java.io.IOException))
            );
        }

        return guarded;
    }

    /**
     * Blocking convenience wrapper. Uses ApiClient.defaultTimeout if timeout == null.
     */
    protected <T> T invoke(
            HttpMethod method,
            String path,
            Object requestBody,
            Class<T> responseType,
            Map<String, String> headers,
            Optional<String> maybeCorrelationId,
            Duration timeout
    ) {
        return invoke(method, path, requestBody, responseType, headers, maybeCorrelationId, timeout, CallOptions.defaults());
    }

    protected <T> T invoke(
            HttpMethod method,
            String path,
            Object requestBody,
            Class<T> responseType,
            Map<String, String> headers,
            Optional<String> maybeCorrelationId,
            Duration timeout,
            CallOptions callOptions
    ) {
        Duration to = timeout != null ? timeout : apiClient.getDefaultTimeout();
        try {
            return invokeAsync(method, path, requestBody, responseType, headers, maybeCorrelationId, callOptions)
                    .block(to);
        } catch (WebClientResponseException wre) {
            // map if not already mapped
            int sc = wre.getRawStatusCode();
            String rawBody;
            try {
                rawBody = wre.getResponseBodyAsString();
            } catch (Exception ex) {
                // 防御性：读取响应体失败时，使用空字符串，保留原始异常作为 cause
                rawBody = "";
            }
            // 截断响应体，避免日志或异常消息过大（这里以 2048 字符为例）
            String body = rawBody == null ? "" : (rawBody.length() > 2048 ? rawBody.substring(0, 2048) + "...(truncated)" : rawBody);

            if (sc >= 400 && sc < 500) {
                // 将原始异常作为 cause 传入，便于调试堆栈信息
                throw new ClientException(sc, body, wre);
            } else if (sc >= 500) {
                throw new ServerException(sc, body, wre);
            } else {
                throw new NetworkException("Unexpected HTTP error", wre);
            }
        } catch (Exception e) {
            throw new NetworkException("Network/timeout error invoking " + path, e);
        }
    }
}
