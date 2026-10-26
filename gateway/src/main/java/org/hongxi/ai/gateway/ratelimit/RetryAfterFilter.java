package org.hongxi.ai.gateway.ratelimit;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

/**
 * 被限流的响应补一个 {@code Retry-After}。
 *
 * <p>spring-cloud-gateway 的 {@code RequestRateLimiter} 拒绝时给的是 429 加一组
 * {@code X-RateLimit-*} 头（Remaining / Replenish-Rate / Burst-Capacity），没有
 * {@code Retry-After}。而 OpenAI 官方 SDK 与多数客户端在 429 上是读 {@code Retry-After} 决定
 * 退避时长的——少了它，客户端只能按自己的固定策略瞎重试，往往正好把桶再打满一次。
 *
 * <p>值取 1 秒：{@code RedisRateLimiter} 的补充粒度就是每秒（replenishRate 是「每秒补几个」），
 * 等一秒一定能拿到令牌。要更精确就得把该路由的 replenishRate 读进来换算，为一个头部值不值得，
 * 先不做。
 *
 * <p>为什么用 {@code beforeCommit} 而不是在链尾改头：限流过滤器拒绝时是直接
 * {@code response.setComplete()} 把响应提交的，链尾的 post-work 跑到时已经晚了。
 * {@code beforeCommit} 的回调在提交前一刻执行，才赶得上。
 */
@Component
public class RetryAfterFilter implements GlobalFilter, Ordered {

    /** 秒。与 {@code RedisRateLimiter} 的每秒补充粒度对齐。 */
    static final String RETRY_AFTER_SECONDS = "1";

    @Override
    public int getOrder() {
        // 晚于 NettyWriteResponseFilter(-1)：它负责最终写响应；早于路由上的限流过滤器，
        // 这样回调一定在限流提交响应之前登记好
        return 0;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        exchange.getResponse().beforeCommit(() -> {
            HttpStatusCode status = exchange.getResponse().getStatusCode();
            // 已经给过的不覆盖：厂商窗口那一维知道自己还剩几秒翻篇，比这里的兜底值准
            if (status != null && status.value() == 429
                    && exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER) == null) {
                exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS);
            }
            return Mono.empty();
        });
        return chain.filter(exchange);
    }
}
