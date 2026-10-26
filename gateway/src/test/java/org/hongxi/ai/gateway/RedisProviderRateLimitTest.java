package org.hongxi.ai.gateway;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.RateLimitTestSupport;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R8 厂商维度端到端：一个厂商一个桶、所有租户共用。这一维才是大模型网关特有的——普通 API 网关
 * 没有「下游共享上游额度」这层关系。
 *
 * <p>与 {@link RedisRateLimitTest} 分成两个类是必须的：那边租户桶小、厂商桶大，这边要反过来，
 * 而配额是网关级 default-filters，一个上下文只有一套数值。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 每个测试类一句自己的标记：Spring 的上下文缓存键只看合并后的配置，
        // 不看 @DynamicPropertySource 贡献的值——两个类的光看注解会算同一个上下文，
        // 路由表就被前一个类复用了（实测整类跑时指标断言会飘）。
        "ai-gateway.test-context=redis-provider-limit",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=6379",
        // 租户桶给到撞不到的大小，只留厂商维度每窗口 2 次生效
        "ai-gateway.rate-limit.tenant-replenish-rate=100",
        "ai-gateway.rate-limit.tenant-burst-capacity=100",
        "ai-gateway.rate-limit.provider-requests-per-window=2",
        "ai-gateway.rate-limit.provider-window-seconds=60",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=shared",
        "ai-gateway.tenants.beta.api-key=sk-beta",
        "ai-gateway.tenants.beta.providers[0]=shared"
})
class RedisProviderRateLimitTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    private GatewayCalls calls;

    @BeforeAll
    static void requireLocalRedis() {
        RateLimitTestSupport.requireLocalRedis();
    }

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        // provider 叫 shared，与另一个类的 provider:stub 不串味
        RateLimitTestSupport.stubRoute(registry, "shared-provider", 0, upstream::baseUrl, CHAT_PATH, "shared",
                "/upstream/chat");
        RateLimitTestSupport.stubRoute(registry, "shared-unused", 1, upstream::baseUrl, "/unused/**", "unused",
                "/upstream/unused");
    }

    @BeforeEach
    void connectAndClearBuckets() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        RateLimitTestSupport.flushBuckets(redis);
    }

    @AfterEach
    void clearBucketsAfterwards() {
        RateLimitTestSupport.flushBuckets(redis);
    }

    @Test
    void providerBucketIsSharedBetweenTenants() {
        assertThat(calls.call(CHAT_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.OK.value());
        assertThat(calls.call(CHAT_PATH, "Bearer sk-beta").status()).isEqualTo(HttpStatus.OK.value());

        Outcome third = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(third.status())
                .as("beta 用掉的那一次也算进 provider:shared 的桶，第三个请求该被拒")
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(upstream.requests()).as("两个租户加起来只放出去两次").isEqualTo(2);
        assertThat(third.headers().getFirst(HttpHeaders.RETRY_AFTER))
                .as("厂商这一维自己算 Retry-After：要等窗口翻篇（固定时钟落在整分钟，所以是 60）")
                .isEqualTo("60");
        assertThat(third.headers().getFirst("X-Provider-RateLimit-Limit")).isEqualTo("2");
        assertThat(third.body()).contains("provider_rate_limit_exceeded");
    }

    /**
     * 固定时钟。厂商窗口按秒切，不钉住某一秒，三个请求跨进两个窗口就会偶然通过——这一维测的是
     * 「同一秒内第三个必被拒」，不能让真实时间参与。
     *
     * <p>换的是注入的 bean，不是给生产代码开后门：{@code ProviderQuotaFilter} 本来就该收一个
     * {@code Clock}，生产注入的是系统时钟。
     */
    @TestConfiguration
    static class FixedWindow {

        @Bean
        @Primary
        Clock fixedGatewayClock() {
            return Clock.fixed(Instant.parse("2026-10-26T12:00:00Z"), ZoneOffset.UTC);
        }
    }
}
