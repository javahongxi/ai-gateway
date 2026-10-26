package org.hongxi.ai.gateway.support;

import java.util.Map;

import reactor.core.publisher.Mono;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * 只给测试用的「永远放行」限流器。
 *
 * <p>为什么需要它：生产配置的租户限流是 {@code default-filters}，而
 * {@code spring.cloud.gateway.server.webflux.default-filters=}（空值）**清不掉** yml 里那份列表
 * ——Boot 对集合属性按索引逐个合并，空值不构成覆盖。实测：连打 40 次之后第 35 次拿到 429，
 * 而那个测试类里明明写着要清空 default-filters。
 *
 * <p>所以不测限流的类都 {@code @Import} 这个：把默认 {@code RateLimiter} 换成永远允许的桩，
 * 既不用真连 Redis，也不会被配额绊住。测限流的那两类（{@code RedisRateLimitTest} /
 * {@code RedisProviderRateLimitTest}）不导入，走的就是生产那条路。
 */
@TestConfiguration(proxyBeanMethods = false)
public class AlwaysAllowRateLimiter {

    @Bean
    @Primary
    public RateLimiter<Object> unlimitedRateLimiter() {
        return new RateLimiter<>() {
            @Override
            public Mono<RateLimiter.Response> isAllowed(String routeId, String id) {
                return Mono.just(new RateLimiter.Response(true, Map.of("X-Test-RateLimiter", "unlimited")));
            }

            @Override
            public Object newConfig() {
                return new Object();
            }

            @Override
            public Class<Object> getConfigClass() {
                return Object.class;
            }

            @Override
            public Map<String, Object> getConfig() {
                // StatefulConfigurable 要的是同步 Map，不是 Mono
                return Map.of();
            }
        };
    }
}
