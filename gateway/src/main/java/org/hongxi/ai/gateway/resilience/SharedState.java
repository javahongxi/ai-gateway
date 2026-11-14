package org.hongxi.ai.gateway.resilience;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import reactor.core.publisher.Mono;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R25：把 key 冷却与熔断开闸信号放进 Redis，让多个网关实例共享「谁被判死了」。形状照 LiteLLM 的
 * {@code coordination_redis}，但守住本网关的两条老规矩：Redis 不可达退回本地（D5），收闸只归本地
 * 探活/半开、远端信号只触发开（D12/D13/D21）。
 *
 * <p><b>两类信号、两种读法，差别在线程。</b>
 * <ul>
 *   <li><b>key 冷却</b>在 {@code UpstreamKeyPool.pick} 里被读，而 pick 跑在 reactor 线程上——所以
 *   {@link #isCooling} <b>绝不阻塞</b>：读本地短 TTL 缓存，缓存过期就异步刷新、本次先返回上次已知值。
 *   跨实例可见性至多滞后一个 {@code local-cache-ttl}，这是不阻塞事件循环换来的，写进文档不藏着。</li>
 *   <li><b>熔断开闸</b>不在请求路径上读：请求绕不绕厂商由本地 resilience4j 熔断决定，远端标记只由
 *   {@code SharedBreakerSync} 在调度线程上定时同步进本地熔断。所以 {@link #isOpen} 可以有界阻塞——
 *   它只给调度线程与测试用。</li>
 * </ul>
 *
 * <p>写一律 fire-and-forget：冷却登记、开闸/收闸标记都不该把已经发生的响应或已经做出的选取卡住。
 * 指纹口径与 {@code UpstreamKeyPool} 一致（末四位），<b>原文绝不进 Redis 键</b>。
 *
 * <p>默认关（{@code ai-gateway.shared-state.enabled=false}）时每个方法都在碰 Redis 之前短路返回，
 * 密钥池据此走纯本地，现有行为逐条不变。
 */
@Component
public class SharedState {

    /** key 冷却键前缀：{@code ai-gateway:cooldown:<provider>:<指纹>}，TTL=冷却时长，自然过期。 */
    static final String COOLDOWN_PREFIX = "ai-gateway:cooldown:";

    /** 熔断开闸键前缀：{@code ai-gateway:breaker:<provider>}，TTL=wait-duration-in-open-state。 */
    static final String BREAKER_PREFIX = "ai-gateway:breaker:";

    /** isOpen 的有界阻塞预算。只在调度线程上花，Redis 半死不活时也不把同步卡死。 */
    private static final Duration READ_TIMEOUT = Duration.ofMillis(500);

    private final ReactiveStringRedisTemplate redis;

    private final AiGatewayProperties properties;

    /** 缓存按时间判新鲜；时钟可注入，测试钉住某一秒才能把「缓存过期触发刷新」写成确定断言。 */
    private final Clock clock;

    /** 远端冷却标记的本地缓存。键是冷却键，值是「在不在 + 何时采的」，按 local-cache-ttl 判新鲜。 */
    private final Map<String, Cached> cooldownCache = new ConcurrentHashMap<>();

    public SharedState(ReactiveStringRedisTemplate redis, AiGatewayProperties properties, Clock clock) {
        this.redis = redis;
        this.properties = properties;
        this.clock = clock;
    }

    public boolean enabled() {
        return properties.getSharedState().isEnabled();
    }

    private record Cached(boolean present, Instant at) {
    }

    /**
     * 登记一把 key 的冷却（本实例刚被上游回 429/自报限流）。本地缓存先置真，再 fire-and-forget 写
     * Redis（TTL=冷却时长）。本实例的 {@code UpstreamKeyPool} 另有自己的本地冷却表，这里只负责让
     * <b>别的</b>实例也能看到。
     */
    public void markCooling(String provider, String fingerprint, Duration ttl) {
        if (!enabled() || ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        String key = cooldownKey(provider, fingerprint);
        cooldownCache.put(key, new Cached(true, clock.instant()));
        redis.opsForValue().set(key, "1", ttl)
                .onErrorResume(redisUnavailable -> Mono.empty())
                .subscribe();
    }

    /**
     * 这把 key 在<b>别的实例</b>那儿是不是正冷却。非阻塞：缓存新鲜就直接用，过期就异步刷新、本次
     * 返回上次已知值（缺省 false）。Redis 不可达时刷新落到 false——宁可少冷却不可错杀，与 D5 同。
     */
    public boolean isCooling(String provider, String fingerprint) {
        if (!enabled()) {
            return false;
        }
        String key = cooldownKey(provider, fingerprint);
        Instant now = clock.instant();
        Cached cached = cooldownCache.get(key);
        Duration ttl = properties.getSharedState().getLocalCacheTtl();
        if (cached != null && cached.at().plus(ttl).isAfter(now)) {
            return cached.present();
        }
        // 缓存过期/缺失：异步刷新，绝不阻塞选取所在的 reactor 线程。本次先返回上次已知值。
        redis.hasKey(key)
                .onErrorResume(redisUnavailable -> Mono.just(false))
                .subscribe(present -> cooldownCache.put(key, new Cached(present, clock.instant())));
        return cached != null && cached.present();
    }

    /** 本地熔断打开时写远端开闸标记（TTL=wait-duration）。fire-and-forget，由熔断状态监听触发。 */
    public void markOpen(String provider, Duration ttl) {
        if (!enabled() || ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        redis.opsForValue().set(breakerKey(provider), "1", ttl)
                .onErrorResume(redisUnavailable -> Mono.empty())
                .subscribe();
    }

    /** 本地熔断收闸时清远端标记（探活治愈/半开恢复），让别的实例也不再绕开这家。fire-and-forget。 */
    public void markClosed(String provider) {
        if (!enabled()) {
            return;
        }
        redis.delete(breakerKey(provider))
                .onErrorResume(redisUnavailable -> Mono.empty())
                .subscribe();
    }

    /**
     * 远端有没有这家的开闸标记。<b>只给调度线程与测试用</b>：有界阻塞读，请求路径不碰它（开闸由
     * {@code SharedBreakerSync} 同步进本地熔断）。Redis 不可达/超时一律 false——绝不因共享存储挂了
     * 就把厂商判死（fail-open，D21）。
     */
    public boolean isOpen(String provider) {
        if (!enabled()) {
            return false;
        }
        try {
            Boolean present = redis.hasKey(breakerKey(provider))
                    .onErrorResume(redisUnavailable -> Mono.just(false))
                    .block(READ_TIMEOUT);
            return Boolean.TRUE.equals(present);
        }
        catch (Exception unavailable) {
            return false;
        }
    }

    static String cooldownKey(String provider, String fingerprint) {
        return COOLDOWN_PREFIX + provider + ":" + fingerprint;
    }

    static String breakerKey(String provider) {
        return BREAKER_PREFIX + provider;
    }
}