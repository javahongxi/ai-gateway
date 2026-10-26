package org.hongxi.ai.gateway.ratelimit;

import java.util.Objects;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;

/**
 * R8 的两个 key。配额本身分在两处：租户维度交给 spring-cloud-gateway 的
 * {@code RequestRateLimiter}（它负责写 {@code X-RateLimit-*} 头），厂商维度在
 * {@link ProviderQuotaFilter} 里自己实现——同一条路由上挂两个 RequestRateLimiter，后一个的配额
 * 会把前一个覆盖掉，见 docs/requirements.md 决策 D5。
 */
@Configuration(proxyBeanMethods = false)
public class RateLimitConfiguration {

    /** 租户维度的 key 前缀。 */
    static final String TENANT_PREFIX = "tenant:";

    /** 厂商维度的 key 前缀。 */
    static final String PROVIDER_PREFIX = "provider:";

    /**
     * 租户维度：鉴权过滤器写进 exchange 的租户名。
     *
     * <p>标 {@code @Primary} 是因为 {@code RequestRateLimiterGatewayFilterFactory} 按类型注入一个
     * {@code KeyResolver} 当默认值，容器里有两个就得说清哪个是默认。
     *
     * <p>解析不出 key 时返回空——鉴权排在限流之前，走到这里的请求一定已经有租户身份，
     * 真空了说明链序被改坏，交给 {@code denyEmptyKey} 的默认行为（403）暴露出来，而不是放过去。
     */
    @Bean
    @Primary
    public KeyResolver tenantKeyResolver() {
        return exchange -> Mono.justOrEmpty(exchange.<String>getAttribute(TenantAuthFilter.TENANT_ATTRIBUTE))
                .map(name -> TENANT_PREFIX + name);
    }

    /** 厂商维度：本条路由 metadata 里的 provider。没有 provider 的路由不受这一维约束。 */
    @Bean
    public KeyResolver providerKeyResolver() {
        return exchange -> Mono.justOrEmpty(routeOf(exchange))
                // mapNotNull 而不是 map + filter：metadata 里没有 provider 时 get 返回 null，
                // 而 Mono.map 的函数返回 null 会直接抛 NPE（这条是被测试逼出来的）
                .mapNotNull(route -> route.getMetadata().get("provider"))
                .map(provider -> PROVIDER_PREFIX + provider);
    }

    static Route routeOf(ServerWebExchange exchange) {
        return exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
    }

    /**
     * 厂商窗口的秒切与租户桶的计时都读这个时钟。生产就是系统时钟；测试里换一个固定瞬间，
     * 才能把「同一个窗口内第三个必被拒」写成不会偶然通过的断言。
     */
    @Bean
    public java.time.Clock gatewayClock() {
        return java.time.Clock.systemUTC();
    }

    /** {@link #providerKeyResolver()} 的同步版本，给 {@link ProviderQuotaFilter} 用。 */
    static String providerOf(ServerWebExchange exchange) {
        Route route = routeOf(exchange);
        if (route == null || route.getMetadata() == null) {
            return null;
        }
        return Objects.toString(route.getMetadata().get("provider"), null);
    }
}
