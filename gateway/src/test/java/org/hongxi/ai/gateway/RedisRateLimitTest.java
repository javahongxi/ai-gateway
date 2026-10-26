package org.hongxi.ai.gateway;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
 * R8 租户维度端到端：桶按租户分开、超配额 429、被限的请求不碰到厂商、状态在 Redis 里。
 *
 * <p>需要本机 Redis（127.0.0.1:6379），连不上整类 skip。
 *
 * <p>配额走的是生产同一套 {@code ai-gateway.rate-limit.*} 占位符：这里把租户桶调成 2、厂商桶调成
 * 100，所以「被限」只能来自租户维度。厂商维度的共用桶在 {@link RedisProviderRateLimitTest}——
 * 那要一组相反的数值，也就另一个 Spring 上下文，两个维度没法共用一个。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 每个测试类一句自己的标记：Spring 的上下文缓存键只看合并后的配置，
        // 不看 @DynamicPropertySource 贡献的值——两个类的光看注解会算同一个上下文，
        // 路由表就被前一个类复用了（实测整类跑时指标断言会飘）。
        "ai-gateway.test-context=redis-tenant-limit",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=6379",
        // 桶容量 2、每秒补 1、每次调用扣 2：第二次一定被拒，因为攒够 2 枚要等 2 秒，
        // 而两个本地调用之间只隔几十毫秒。写成「每次扣 1、第三次被拒」就是掷骰子——
        // 三次调用要跑 0.5~1.8 秒，跨过秒界就补回一枚令牌，第三次反而放行（实测飘过）
        "ai-gateway.rate-limit.tenant-replenish-rate=1",
        "ai-gateway.rate-limit.tenant-burst-capacity=2",
        "ai-gateway.rate-limit.tenant-requested-tokens=2",
        // 厂商维度给到撞不到的大小，只留租户桶生效
        "ai-gateway.rate-limit.provider-requests-per-window=1000",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub",
        "ai-gateway.tenants.beta.api-key=sk-beta",
        "ai-gateway.tenants.beta.providers[0]=stub"
})
class RedisRateLimitTest {

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
        RateLimitTestSupport.stubRoute(registry, "stub-chat", 0, upstream::baseUrl, CHAT_PATH, "stub",
                "/upstream/chat");
        RateLimitTestSupport.stubRoute(registry, "stub-unused", 1, upstream::baseUrl, "/unused/**", "unused",
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
    void tenantBucketDeniesOnceItsBurstIsGone() {
        assertThat(calls.call(CHAT_PATH, "Bearer sk-alpha").status())
                .as("第一次把 2 的桶扣空，放行")
                .isEqualTo(HttpStatus.OK.value());

        Outcome second = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(second.status()).as("桶已空，第二次就该被拒").isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(upstream.requests()).as("被限的请求不能碰到厂商，否则限了个寂寞").isEqualTo(1);
    }

    @Test
    void denialCarriesRetryAfterAndTheRemainingTokensHeader() {
        calls.call(CHAT_PATH, "Bearer sk-alpha");
        Outcome denied = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(denied.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(denied.headers().getFirst(HttpHeaders.RETRY_AFTER))
                .as("SCG 自己不给 Retry-After，客户端就不会退避")
                .isEqualTo("1");
        assertThat(denied.headers().toSingleValueMap()).containsKey("X-RateLimit-Remaining");
    }

    @Test
    void anotherTenantKeepsItsOwnBucket() {
        calls.call(CHAT_PATH, "Bearer sk-alpha");
        assertThat(calls.call(CHAT_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());

        assertThat(calls.call(CHAT_PATH, "Bearer sk-beta").status())
                .as("桶按租户分开，alpha 打满不该牵连 beta")
                .isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void bucketStateLivesInRedisNotInTheJvm() {
        calls.call(CHAT_PATH, "Bearer sk-alpha");

        List<String> keys = redis.keys("request_rate_limiter.*tenant:alpha*").collectList().block(Duration.ofSeconds(5));

        assertThat(keys)
                .as("配额状态在 Redis 里，多个网关实例才共享同一份桶")
                .isNotEmpty()
                .allSatisfy(key -> assertThat(key).contains("tenant:alpha"));
    }
}
