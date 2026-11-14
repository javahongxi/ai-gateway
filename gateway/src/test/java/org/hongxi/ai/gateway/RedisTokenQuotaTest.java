package org.hongxi.ai.gateway;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
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
 * R23 端到端：租户 token 配额的「预扣 + 结算」。配额 100、预扣额 60（{@code max_tokens} 或默认），
 * 固定时钟钉住一个窗口。结算复用响应观察者采到的 usage（D11/D19），所以这里验的是真实回灌：
 * 带 usage 的调用结算后窗口计的是 18（11+7）而不是预扣的 60。
 *
 * <p>结算是 fire-and-forget 的异步 Redis 写，客户端收到响应那一刻它未必落地——所以断言窗口计数
 * 用轮询等它稳定（{@link #awaitTokenCount}），而不是拿真实时间赌时序。窗口本身由固定时钟钉死。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=redis-token-quota",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=6379",
        // 请求数桶给到撞不到，只留 token 维度生效；厂商维度关掉
        "ai-gateway.rate-limit.tenant-replenish-rate=100",
        "ai-gateway.rate-limit.tenant-burst-capacity=100",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.rate-limit.tenant-tokens-per-window=100",
        "ai-gateway.rate-limit.token-window-seconds=60",
        "ai-gateway.rate-limit.default-token-reservation=60",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub"
})
class RedisTokenQuotaTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

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
        RateLimitTestSupport.stubRoute(registry, "token-provider", 0, upstream::baseUrl, CHAT_PATH, "stub",
                "/upstream/chat");
    }

    @BeforeEach
    void connectAndClear() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, null);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        RateLimitTestSupport.flushBuckets(redis);
    }

    @AfterEach
    void clearAfterwards() {
        RateLimitTestSupport.flushBuckets(redis);
    }

    private Outcome chat(String body) {
        return calls.call(CHAT_PATH, "Bearer sk-alpha", body);
    }

    private static String bodyWithMaxTokens(int maxTokens) {
        return "{\"model\":\"stub-model\",\"max_tokens\":" + maxTokens
                + ",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
    }

    /** 带 usage 的调用：预扣 60，结算回灌成真实用量 18（11+7），而不是停在 60。 */
    @Test
    void settlementReplacesReservationWithRealUsage() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON_WITH_USAGE);

        assertThat(chat(bodyWithMaxTokens(60)).status()).isEqualTo(HttpStatus.OK.value());

        awaitTokenCount(18);
        assertThat(upstream.requests()).isEqualTo(1);
    }

    /** 无 usage 时保留预扣（保守）：两发 60 叠到 120 > 100，第二发 429，上游只收到一次。 */
    @Test
    void reservationsStackUntilQuotaExhaustedWhenUsageAbsent() {
        assertThat(chat(bodyWithMaxTokens(60)).status()).isEqualTo(HttpStatus.OK.value());
        awaitTokenCount(60);

        Outcome second = chat(bodyWithMaxTokens(60));

        assertThat(second.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(second.headers().getFirst("X-Tenant-TokenLimit-Limit")).isEqualTo("100");
        assertThat(second.headers().getFirst(HttpHeaders.RETRY_AFTER))
                .as("固定时钟落在整分钟，窗口剩余 60 秒")
                .isEqualTo("60");
        assertThat(second.body()).contains("tenant_token_limit_exceeded");
        assertThat(upstream.requests()).as("被拒的那发没碰到上游").isEqualTo(1);
        // 超限退回：窗口计数仍是第一发的 60，不是 120
        awaitTokenCount(60);
    }

    /** 上游回非 2xx：预扣全额退回（这次调用没产出计费 completion），下一发照样放得出去。 */
    @Test
    void nonSuccessRefundsTheReservation() {
        upstream.scenario(StubUpstream.Scenario.FAIL_500);
        assertThat(chat(bodyWithMaxTokens(60)).status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        awaitTokenCount(0);

        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        assertThat(chat(bodyWithMaxTokens(60)).status())
                .as("上一发已退回，这一发预扣 60 仍在配额内")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.requests()).isEqualTo(2);
    }

    /** 没带 max_tokens 时用默认预扣额 60：第一发 200、第二发 429，与显式带 60 同形状。 */
    @Test
    void defaultReservationAppliesWhenMaxTokensAbsent() {
        String body = "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
        assertThat(chat(body).status()).isEqualTo(HttpStatus.OK.value());
        awaitTokenCount(60);

        assertThat(chat(body).status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(upstream.requests()).isEqualTo(1);
    }

    /**
     * 轮询窗口计数直到等于期望值。结算是异步 Redis 写，客户端返回时它未必落地；固定时钟保证
     * 只有一个窗口键，读它即可。最多等 2 秒——等的是 Redis 完成，不是让真实时间参与窗口判定。
     */
    private void awaitTokenCount(long expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        long actual = -1;
        while (System.nanoTime() < deadline) {
            actual = currentTokenCount();
            if (actual == expected) {
                return;
            }
            try {
                Thread.sleep(20);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(actual).as("token 窗口计数应结算到 %d", expected).isEqualTo(expected);
    }

    private long currentTokenCount() {
        List<String> keys = redis.keys(RateLimitTestSupport.TOKEN_WINDOW_KEY_PATTERN)
                .collectList().block(Duration.ofSeconds(5));
        if (keys == null || keys.isEmpty()) {
            return 0;
        }
        long total = 0;
        for (String key : keys) {
            String value = redis.opsForValue().get(key).block(Duration.ofSeconds(5));
            if (value != null) {
                total += Long.parseLong(value);
            }
        }
        return total;
    }

    /** 固定时钟：窗口按秒切，钉住整分钟才能把 Retry-After=60 与「同一窗口内叠加」写成确定断言。 */
    @TestConfiguration
    static class FixedWindow {

        @Bean
        @Primary
        Clock fixedGatewayClock() {
            return Clock.fixed(Instant.parse("2026-10-26T12:00:00Z"), ZoneOffset.UTC);
        }
    }
}