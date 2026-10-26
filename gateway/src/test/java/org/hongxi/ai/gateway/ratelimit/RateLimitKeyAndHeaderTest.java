package org.hongxi.ai.gateway.ratelimit;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 限流的两个自建零件：key 解析与 {@code Retry-After}。不依赖 Redis，每次构建都跑。
 *
 * <p>端到端的 429 行为（配额、租户隔离、厂商共用）在 {@code RedisRateLimitTest}——那要连 Redis，
 * 缺就 skip。两条维度各自独立的配额也只能在那里证明。
 */
class RateLimitKeyAndHeaderTest {

    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(1);

    private final RateLimitConfiguration configuration = new RateLimitConfiguration();

    @Test
    void tenantKeyIsPrefixedSoItCannotCollideWithTheProviderBucket() {
        MockServerWebExchange exchange = exchangeWithTenant("team-a");

        assertThat(configuration.tenantKeyResolver().resolve(exchange).block()).isEqualTo("tenant:team-a");
    }

    @Test
    void tenantKeyIsEmptyWhenTheRequestHasNoIdentity() {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());

        // 空 key 交给 RequestRateLimiter 的 denyEmptyKey（默认 403）。鉴权正常时到不了这里，
        // 真到了说明链序被改坏，宁可暴露也不要放行
        assertThat(configuration.tenantKeyResolver().resolve(exchange).block()).isNull();
    }

    @Test
    void providerKeyComesFromTheRouteMetadata() {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());
        exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR, routeWithProvider("dashscope"));

        assertThat(configuration.providerKeyResolver().resolve(exchange).block()).isEqualTo("provider:dashscope");
    }

    @Test
    void providerKeyIsEmptyOnARouteWithoutAProvider() {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());
        exchange.getAttributes().put(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR,
                Route.async().id("r").uri("https://example.com").predicate(request -> true).build());

        // 没标 provider 的路由不该挂厂商维度限流；挂了会因为空 key 被拒，看得见
        assertThat(configuration.providerKeyResolver().resolve(exchange).block()).isNull();
    }

    @Test
    void deniedResponseGetsRetryAfterSoClientsBackOffInsteadOfHammering() {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);

        denyThenCommit(exchange);

        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.RETRY_AFTER))
                .as("OpenAI 客户端在 429 上读这个头决定退避；限流过滤器自己不给")
                .isEqualTo("1");
    }

    @Test
    void allowedResponseIsLeftAlone() {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());
        exchange.getResponse().setStatusCode(HttpStatus.OK);

        denyThenCommit(exchange);

        assertThat(exchange.getResponse().getHeaders().toSingleValueMap()).doesNotContainKey(HttpHeaders.RETRY_AFTER);
    }

    /**
     * 走一遍过滤器再提交响应。必须先跑过滤器：它注册的是 {@code beforeCommit} 回调，
     * 而限流拒绝时是直接 {@code setComplete()} 提交响应的，链尾改头已经来不及。
     */
    private void denyThenCommit(MockServerWebExchange exchange) {
        new RetryAfterFilter().filter(exchange, chained -> Mono.empty()).block();
        exchange.getResponse().setComplete().block(COMMIT_TIMEOUT);
    }

    private static MockServerWebExchange exchangeWithTenant(String tenant) {
        MockServerWebExchange exchange = MockServerWebExchange
                .from(MockServerHttpRequest.post("/v1/chat/completions").build());
        exchange.getAttributes().put(TenantAuthFilter.TENANT_ATTRIBUTE, tenant);
        return exchange;
    }

    private static Route routeWithProvider(String provider) {
        return Route.async()
                .id("r")
                .uri("https://example.com")
                .predicate(exchange -> true)
                .metadata(Map.of("provider", provider))
                .build();
    }
}
