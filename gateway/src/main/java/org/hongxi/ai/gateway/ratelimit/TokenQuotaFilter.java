package org.hongxi.ai.gateway.ratelimit;

import java.nio.charset.StandardCharsets;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;
import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.routing.RequestBodyModels;

/**
 * R23：租户 token 维度的请求侧闸门。请求前按 {@code max_tokens} 预扣（{@link TokenQuotaService}），
 * 超限就 429；放行时把预约写进 exchange 属性，响应后由 {@code ResponseObservationFilter} 按真实
 * usage 结算（D19）。
 *
 * <p><b>order 95</b>：紧邻厂商维度的 {@code ProviderQuotaFilter}(100)，在鉴权之后、上游之前。
 * 它晚于路由内过滤器（{@code ModelMapping} 等），所以请求体已被缓存——取 {@code max_tokens} 只读
 * 缓存字节，不重放、不改报文（{@link RequestBodyModels#readBody}）。
 *
 * <p><b>配额 0（默认）时整条路径不读体、不碰 Redis</b>：现有行为一字不变。这是刻意的加法式落地——
 * 没被显式启用就不该改动那些踩坑换来的结论。
 *
 * <p>预约属性<b>请求阶段写</b>，所以响应侧外层 {@code doFinally} 读得到（D11：内层 doFinally 写的
 * 属性外层读不到，请求阶段写的能）。被本过滤器自己 429 的请求没写属性，结算什么都不做。
 */
@Component
public class TokenQuotaFilter implements GlobalFilter, Ordered {

    /** 预扣预约挂到 exchange 上的键，响应侧结算读它。请求阶段写，外层读得到（D11）。 */
    public static final String RESERVATION_ATTRIBUTE = "aiGatewayTokenReservation";

    private final TokenQuotaService quota;

    private final AiGatewayProperties properties;

    public TokenQuotaFilter(TokenQuotaService quota, AiGatewayProperties properties) {
        this.quota = quota;
        this.properties = properties;
    }

    @Override
    public int getOrder() {
        // 晚于 NettyWriteResponseFilter(-1) 才能自己把 429 写出去；早于 RouteToRequestUrlFilter(10000)，
        // 被拒的请求不会碰到上游。与厂商维度(100)相邻，先扣租户 token 再扣厂商请求数。
        return 95;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        if (quota.limit() <= 0) {
            return chain.filter(exchange);
        }
        String tenant = exchange.getAttribute(TenantAuthFilter.TENANT_ATTRIBUTE);
        if (tenant == null) {
            // 鉴权排在前面，走到这里一定有身份；真空了说明链序被改坏，放行交给后面的闸门暴露
            return chain.filter(exchange);
        }
        // R24 的请求内重试会重跑本过滤器（Retry 在路由链最前，包着 order 95 的本闸门）：
        // 预约只记一次，复用已有的，避免同一请求重试几次就预扣几次。reset() 不清这个属性，所以读得到。
        if (exchange.getAttributes().containsKey(RESERVATION_ATTRIBUTE)) {
            return chain.filter(exchange);
        }
        long fallback = properties.getRateLimit().getDefaultTokenReservation();
        return RequestBodyModels.readBody(exchange)
                .flatMap(body -> {
                    long amount = RequestBodyModels.maxTokensOf(body.bytes(), fallback);
                    return quota.reserve(tenant, amount).flatMap(reservation -> {
                        if (!reservation.allowed()) {
                            return deny(exchange, reservation.limit());
                        }
                        if (reservation.tracked()) {
                            exchange.getAttributes().put(RESERVATION_ATTRIBUTE, reservation);
                        }
                        return chain.filter(body.continued());
                    });
                });
    }

    private Mono<Void> deny(ServerWebExchange exchange, long limit) {
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        exchange.getResponse().getHeaders().set("X-Tenant-TokenLimit-Limit", String.valueOf(limit));
        // 这一维自己算 Retry-After：要等窗口翻篇，RetryAfterFilter 的兜底不覆盖已给过的值
        exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER, String.valueOf(quota.retryAfterSeconds()));
        byte[] body = ("{\"error\":{\"message\":\"Tenant token quota is exhausted, retry shortly.\","
                + "\"type\":\"rate_limit_error\",\"param\":null,\"code\":\"tenant_token_limit_exceeded\"}}")
                .getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse()
                .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}