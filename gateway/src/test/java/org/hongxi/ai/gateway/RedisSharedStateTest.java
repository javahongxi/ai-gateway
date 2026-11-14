package org.hongxi.ai.gateway;

import java.time.Clock;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.resilience.ProviderHealthProbe;
import org.hongxi.ai.gateway.resilience.SharedBreakerSync;
import org.hongxi.ai.gateway.resilience.SharedState;
import org.hongxi.ai.gateway.routing.UpstreamKeyPool;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.RateLimitTestSupport;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R25 端到端：冷却/熔断状态跨实例共享。用<b>两个各带自己 SharedState 的 UpstreamKeyPool</b> 模拟两台
 * 网关实例共享同一 Redis——A 登记的冷却 B 看得到，TTL 到期消失；熔断开闸标记 markOpen/isOpen/markClosed
 * 走通，定时同步<b>只开不关</b>，探活治愈顺手清远端标记。
 *
 * <p>这里<b>不</b>钉固定时钟：冷却读靠本地短 TTL 缓存的异步刷新、Redis 键靠真实 TTL 过期，两者都无法用
 * 注入时钟伪造。所以断言用轮询等状态稳定（{@link #await}），把 local-cache-ttl 调到 100ms、把要验过期的
 * TTL 调到 1s，让等待有界且短。这与限流用例「固定时钟钉窗口」是不同性质：那测的是窗口切分，这测的是
 * 异步共享与到期，真实时间参与是机制本身的一部分，不是赌时序。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=redis-shared-state",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=6379",
        "ai-gateway.generated-routes=false",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        // 共享打开；同步间隔拉到极大，测试自己调 syncOpenBreakers()，不让调度器参与断言
        "ai-gateway.shared-state.enabled=true",
        "ai-gateway.shared-state.sync-interval=3600s",
        "ai-gateway.shared-state.local-cache-ttl=100ms",
        "ai-gateway.probe.enabled=false",
        // pooled：多 key、长冷却，验跨实例共享；short：1s 冷却，验 TTL 过期
        "ai-gateway.providers.pooled.base-url=http://127.0.0.1:1",
        "ai-gateway.providers.pooled.chat-path=/v1/chat/completions",
        "ai-gateway.providers.pooled.keys[0]=sk-aaaa-1111",
        "ai-gateway.providers.pooled.keys[1]=sk-bbbb-2222",
        "ai-gateway.providers.pooled.key-cooldown=30s",
        "ai-gateway.providers.short.base-url=http://127.0.0.1:1",
        "ai-gateway.providers.short.chat-path=/v1/chat/completions",
        "ai-gateway.providers.short.keys[0]=sk-cccc-3333",
        "ai-gateway.providers.short.key-cooldown=1s",
        // probed：探活治愈用，base-url 指向桩
        "ai-gateway.providers.probed.chat-path=/v1/chat/completions",
        "ai-gateway.providers.probed.key-ref=probed-secret",
        "ai-gateway.providers.probed.health-path=/models/probed"
})
@Import(AlwaysAllowRateLimiter.class)
class RedisSharedStateTest {

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private ReactiveStringRedisTemplate redis;

    @Autowired
    private AiGatewayProperties properties;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    @Autowired
    private SharedBreakerSync sync;

    @Autowired
    private ProviderHealthProbe probe;

    @Autowired
    private Clock clock;

    @BeforeAll
    static void requireLocalRedis() {
        RateLimitTestSupport.requireLocalRedis();
    }

    @DynamicPropertySource
    static void stubAddress(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.providers.probed.base-url", upstream::baseUrl);
    }

    @BeforeEach
    void startClean() {
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        RateLimitTestSupport.flushBuckets(redis);
    }

    @AfterEach
    void clearAfterwards() {
        RateLimitTestSupport.flushBuckets(redis);
    }

    /** 一台实例一份 SharedState（各自的本地缓存），共享同一 Redis——这就是「两个网关实例」。 */
    private SharedState newShared() {
        return new SharedState(redis, properties, clock);
    }

    private UpstreamKeyPool newPool(SharedState shared) {
        return new UpstreamKeyPool(properties, new SimpleMeterRegistry(), clock, shared);
    }

    /** A 冷却某 key，B 的 isCooling 该变真（非阻塞读，轮询等异步刷新落地）。 */
    @Test
    void cooldownIsSharedAcrossTwoPools() {
        UpstreamKeyPool a = newPool(newShared());
        UpstreamKeyPool b = newPool(newShared());

        a.coolDown("pooled", "sk-aaaa-1111");

        assertThat(a.isCooling("pooled", "sk-aaaa-1111")).as("本实例本地冷却立刻可见").isTrue();
        await("B 该看到 A 登记的冷却", () -> b.isCooling("pooled", "sk-aaaa-1111"), true);
        assertThat(b.pick("pooled")).as("B 选取时该让开冷却中的那把").isEqualTo("sk-bbbb-2222");
    }

    /** 冷却键 TTL=冷却时长：到期后 B 不再认为它在冷却（1s TTL，轮询等过期 + 缓存刷新）。 */
    @Test
    void cooldownExpiresWithTtl() {
        UpstreamKeyPool a = newPool(newShared());
        UpstreamKeyPool b = newPool(newShared());

        a.coolDown("short", "sk-cccc-3333");

        await("先看到冷却", () -> b.isCooling("short", "sk-cccc-3333"), true);
        await("TTL 到期后冷却消失", () -> b.isCooling("short", "sk-cccc-3333"), false);
    }

    /** markOpen 后 isOpen 真、markClosed 后假。 */
    @Test
    void breakerMarkOpenIsOpenAndMarkClosed() {
        SharedState shared = newShared();

        shared.markOpen("pooled", Duration.ofSeconds(30));
        await("markOpen 后 isOpen", () -> shared.isOpen("pooled"), true);

        shared.markClosed("pooled");
        await("markClosed 后 isOpen 假", () -> shared.isOpen("pooled"), false);
    }

    /** 开闸标记 TTL 到期自然消失，不需要谁去删。 */
    @Test
    void breakerMarkExpiresWithTtl() {
        SharedState shared = newShared();

        shared.markOpen("short", Duration.ofSeconds(1));
        await("先开", () -> shared.isOpen("short"), true);
        await("TTL 到期自然消失", () -> shared.isOpen("short"), false);
    }

    /** 本地熔断 →OPEN 写远端标记、→CLOSED 删（监听写侧）。 */
    @Test
    void listenerWritesRemoteMarkOnLocalTransition() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("pooled");
        SharedState shared = newShared();

        breaker.transitionToOpenState();
        await("本地开闸→远端标记", () -> shared.isOpen("pooled"), true);

        breaker.transitionToClosedState();
        await("本地收闸→远端标记清", () -> shared.isOpen("pooled"), false);
    }

    /** 定时同步只开不关：远端标记在就开本地闸；远端清了也不收本地闸（收闸归探活/半开，D12/D13）。 */
    @Test
    void syncOpensLocalBreakerButNeverCloses() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("pooled");
        SharedState shared = newShared();
        shared.markOpen("pooled", Duration.ofSeconds(30));
        await("远端标记在", () -> shared.isOpen("pooled"), true);

        sync.syncOpenBreakers();
        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // 同步开闸触发监听再写一次标记，等它落地后再清，避免与异步写竞态
        await("同步开闸后标记仍在", () -> shared.isOpen("pooled"), true);

        shared.markClosed("pooled");
        await("远端标记已清", () -> shared.isOpen("pooled"), false);
        sync.syncOpenBreakers();
        assertThat(breaker.getState()).as("只开不关：远端清了也不收本地闸").isEqualTo(CircuitBreaker.State.OPEN);
    }

    /** 探活治愈：本地收闸 + 顺手清远端开闸标记，让别的实例也不再绕开这家。 */
    @Test
    void probeHealClearsRemoteMark() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("probed");
        breaker.transitionToOpenState();
        SharedState shared = newShared();
        await("开闸标记在", () -> shared.isOpen("probed"), true);

        probe.probeAll();

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        await("探活治愈清远端标记", () -> shared.isOpen("probed"), false);
    }

    /** Redis 不可达：写不抛错、远端读一律 false（fail-open），本实例本地冷却照常——不把 Redis 可用性乘进网关。 */
    @Test
    void redisUnavailableFallsBackToLocal() {
        LettuceConnectionFactory factory =
                new LettuceConnectionFactory(new RedisStandaloneConfiguration("127.0.0.1", 1));
        factory.afterPropertiesSet();
        try {
            ReactiveStringRedisTemplate badRedis = new ReactiveStringRedisTemplate(factory);
            SharedState bad = new SharedState(badRedis, properties, clock);

            bad.markCooling("pooled", "****1111", Duration.ofSeconds(30));
            bad.markOpen("pooled", Duration.ofSeconds(30));
            // 没本地登记过的 key：远端读不到当不冷却（本实例刚 markCooling 的那把本地缓存知道，不算远端读）
            assertThat(bad.isCooling("pooled", "****9999")).as("远端读不到当不冷却").isFalse();
            assertThat(bad.isOpen("pooled")).as("远端读不到当不开闸").isFalse();

            UpstreamKeyPool pool = newPool(bad);
            pool.coolDown("pooled", "sk-aaaa-1111");
            assertThat(pool.isCooling("pooled", "sk-aaaa-1111")).as("Redis 挂了本地冷却仍在").isTrue();
        }
        finally {
            factory.destroy();
        }
    }

    /**
     * 轮询直到条件等于期望值。共享是异步的（fire-and-forget 写 + 缓存异步刷新 + Redis TTL），客户端/调用方
     * 返回那一刻状态未必落地；最多等 4 秒——等的是 Redis 与刷新完成，不是让真实时间参与窗口判定。
     */
    private static void await(String what, BooleanSupplier condition, boolean expected) {
        long deadline = System.nanoTime() + Duration.ofSeconds(4).toNanos();
        boolean actual = condition.getAsBoolean();
        while (actual != expected && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
            actual = condition.getAsBoolean();
        }
        assertThat(actual).as(what).isEqualTo(expected);
    }
}