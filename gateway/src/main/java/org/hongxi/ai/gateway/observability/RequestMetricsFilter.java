package org.hongxi.ai.gateway.observability;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;

/**
 * R9 的自建指标：按租户与厂商统计请求数、耗时与状态。
 *
 * <p>spring-cloud-gateway 自己的 {@code spring.cloud.gateway.requests} 已经按 routeId、状态、
 * 异常打了点，但它没有「租户」和「厂商」这两个维度——而这两个恰恰是大模型网关要看的：谁在花钱、
 * 各家的延迟与错误率。补一个自己的 Timer 比去改它的约定规范更省事，也更清楚。
 *
 * <p>记的是整条请求的墙钟时间：{@code chain.filter(...)} 的 Mono 在响应写完之后才完成，所以
 * 流式请求统计到的是「从收到请求到最后一个字节推给客户端」。首字延迟（TTFT）与 token 用量要挂钩
 * 正文流，那是响应体层面的事，见文档里 R9 的边界说明。
 *
 * <p>顺序在鉴权之前（{@code HIGHEST_PRECEDENCE}，鉴权是 {@code +10}）：被 401 / 403 挡掉的请求
 * 也要计数，否则「某租户一直拿错 key 打网关」这种最难查的现象会在指标里隐身。鉴权短路时不再往下
 * 走链，所以只有排在前面的过滤器看得见那次拒绝。
 */
@Component
public class RequestMetricsFilter implements GlobalFilter, Ordered {

    /** 指标名。测试与看板都按它取数，所以是公开常量。 */
    public static final String REQUEST_METRIC = "ai.gateway.requests";

    private final MeterRegistry registry;

    public RequestMetricsFilter(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long started = System.nanoTime();
        return chain.filter(exchange).doFinally(signal -> record(exchange, started));
    }

    private void record(ServerWebExchange exchange, long startedNanos) {
        Object tenant = exchange.getAttributes().get(TenantAuthFilter.TENANT_ATTRIBUTE);
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        String provider = route == null ? "none" : String.valueOf(route.getMetadata().get("provider"));
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        Timer.builder(REQUEST_METRIC)
                .description("网关处理的模型调用：按租户、厂商、路由与状态维度")
                .tag("tenant", tenant == null ? "anonymous" : tenant.toString())
                .tag("provider", provider)
                .tag("route", route == null ? "none" : route.getId())
                .tag("status", status == null ? "none" : String.valueOf(status.value()))
                .register(registry)
                .record(System.nanoTime() - startedNanos, TimeUnit.NANOSECONDS);
    }
}
