package com.qifa.myhttpclient.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qifa.myhttpclient.client.ApiClient;
import com.qifa.myhttpclient.CorrelationIdUtil;
import com.qifa.myhttpclient.exceptions.ClientException;
import com.qifa.myhttpclient.exceptions.ServerException;
import com.qifa.myhttpclient.exceptions.NetworkException;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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

    protected DefaultApi(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.mapper = apiClient.getMapper();
    }

    /**
     * Call options controlling retry and simple concurrency-based rate limiting.
     * By default both retry and rate limiting are disabled.
     */
    public static final class CallOptions {
        private final boolean enableRetry;
        private final int maxRetries;
        private final long retryBackoffMillis;

        private final boolean enableRateLimit;
        private final int maxConcurrentRequests; // treated as semaphore permits
        private final long rateAcquireTimeoutMillis; // timeout to acquire permit

        private CallOptions(boolean enableRetry, int maxRetries, long retryBackoffMillis,
                            boolean enableRateLimit, int maxConcurrentRequests, long rateAcquireTimeoutMillis) {
            this.enableRetry = enableRetry;
            this.maxRetries = maxRetries;
            this.retryBackoffMillis = retryBackoffMillis;
            this.enableRateLimit = enableRateLimit;
            this.maxConcurrentRequests = maxConcurrentRequests;
            this.rateAcquireTimeoutMillis = rateAcquireTimeoutMillis;
        }

        public static Builder builder() { return new Builder(); }

        public static CallOptions defaults() { return new Builder().build(); }

        public boolean isEnableRetry() { return enableRetry; }
        public int getMaxRetries() { return maxRetries; }
        public long getRetryBackoffMillis() { return retryBackoffMillis; }
        public boolean isEnableRateLimit() { return enableRateLimit; }
        public int getMaxConcurrentRequests() { return maxConcurrentRequests; }
        public long getRateAcquireTimeoutMillis() { return rateAcquireTimeoutMillis; }

        public static final class Builder {
            private boolean enableRetry = false;
            private int maxRetries = 3;
            private long retryBackoffMillis = 500;
            private boolean enableRateLimit = false;
            private int maxConcurrentRequests = 50;
            private long rateAcquireTimeoutMillis = 1000;

            public Builder enableRetry(boolean enable) { this.enableRetry = enable; return this; }
            public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
            public Builder retryBackoffMillis(long ms) { this.retryBackoffMillis = ms; return this; }
            public Builder enableRateLimit(boolean enable) { this.enableRateLimit = enable; return this; }
            public Builder maxConcurrentRequests(int max) { this.maxConcurrentRequests = max; return this; }
            public Builder rateAcquireTimeoutMillis(long ms) { this.rateAcquireTimeoutMillis = ms; return this; }
            public CallOptions build() {
                return new CallOptions(enableRetry, maxRetries, retryBackoffMillis, enableRateLimit, maxConcurrentRequests, rateAcquireTimeoutMillis);
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

        // retry handling (only retry on network or request IO-like exceptions)
        if (callOptions != null && callOptions.isEnableRetry()) {
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
            String body = wre.getResponseBodyAsString();
            if (sc >= 400 && sc < 500) {
                throw new ClientException(sc, body);
            } else if (sc >= 500) {
                throw new ServerException(sc, body);
            } else {
                throw new NetworkException("Unexpected HTTP error", wre);
            }
        } catch (Exception e) {
            throw new NetworkException("Network/timeout error invoking " + path, e);
        }
    }
}
