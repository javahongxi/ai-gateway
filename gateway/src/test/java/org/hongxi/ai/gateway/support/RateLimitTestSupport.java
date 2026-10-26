package org.hongxi.ai.gateway.support;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import reactor.core.publisher.Flux;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 限流端到端测试共用的三件事：把路由指向进程内桩、确认本机 Redis 在不在、用例之间清桶。
 *
 * <p>{@code stubRoute} 刻意与 application.yml 里的形状对齐（两个字符串过滤器、同样的 metadata
 * 键）。Boot 的属性合并是按索引逐个属性来的：测试里把 routes[0].filters[1] 从字符串换成
 * name/args 映射，或只声明 routes[0] 让 routes[1] 留在 yml 里，混绑出来的结果不可读——
 * 第一版就是这么撞出 UnboundConfigurationPropertiesException 的。
 */
public final class RateLimitTestSupport {

    /** RedisRateLimiter 在 Redis 里的键前缀，用来清场，也用来证明状态真的在库里。 */
    public static final String BUCKET_KEY_PATTERN = "request_rate_limiter.*";

    /** ProviderQuotaFilter 的固定窗口键，同样要清场。 */
    public static final String PROVIDER_WINDOW_KEY_PATTERN = "ai-gateway:provider:*";

    private RateLimitTestSupport() {
    }

    public static void stubRoute(DynamicPropertyRegistry registry, String id, int index, Supplier<String> baseUrl,
            String path, String provider, String upstreamPath) {
        String route = "spring.cloud.gateway.server.webflux.routes[" + index + "]";
        registry.add(route + ".id", () -> id);
        // 包一层 lambda：DynamicPropertyRegistry.add 收的是 Supplier<Object>，
        // 直接把 Supplier<String> 传进去过不了泛型
        registry.add(route + ".uri", () -> baseUrl.get());
        registry.add(route + ".predicates[0]", () -> "Path=" + path);
        registry.add(route + ".metadata.provider", () -> provider);
        registry.add(route + ".filters[0]", () -> "SetPath=" + upstreamPath);
        registry.add(route + ".filters[1]", () -> "SetRequestHeader=Authorization,Bearer provider-secret");
    }

    /** 限流的状态就在 Redis 里，没有它这些用例没有意义，所以整类 skip 而不是换个假桶。 */
    public static void requireLocalRedis() {
        assumeTrue(redisReachable(), "本机 127.0.0.1:6379 没有 Redis，跳过限流端到端");
    }

    private static boolean redisReachable() {
        try (var socket = new java.net.Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 6379), 300);
            return true;
        }
        catch (IOException unreachable) {
            return false;
        }
    }

    /** 用例之间清场：桶是全局状态，上一个用例的消耗不该算到下一个头上。 */
    public static void flushBuckets(ReactiveStringRedisTemplate redis) {
        List<String> keys = redis.keys(BUCKET_KEY_PATTERN).collectList().block(Duration.ofSeconds(5));
        List<String> windows = redis.keys(PROVIDER_WINDOW_KEY_PATTERN).collectList().block(Duration.ofSeconds(5));
        List<String> all = new java.util.ArrayList<>();
        if (keys != null) {
            all.addAll(keys);
        }
        if (windows != null) {
            all.addAll(windows);
        }
        if (!all.isEmpty()) {
            redis.delete(Flux.fromIterable(all)).block(Duration.ofSeconds(5));
        }
    }
}
