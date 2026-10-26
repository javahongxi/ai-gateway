package org.hongxi.ai.gateway.ratelimit;

import java.time.Clock;
import java.time.Duration;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R8 厂商维度：一个厂商一个桶、所有租户共用，因为上游给的是账户级配额。普通 API 网关没有这一维，
 * 大模型网关必须有——否则两个租户各自没打满自己的额度，合起来却把厂商账户打到 429，
 * 惩罚落在所有人头上。
 *
 * <p>为什么不复用 {@code RequestRateLimiter}：同一条路由上挂两个，两个过滤器都会发
 * {@code FilterArgsEvent}，而每个 {@code RedisRateLimiter} bean 都在监听所有事件、按 routeId 存配额。
 * 结果后挂的那个把前一个的配额覆盖掉，两条维度用同一份数值。实测见 docs/requirements.md 决策 D5。
 *
 * <p>实现是**固定窗口**计数（一个窗口一个键），不是令牌桶：窗口边沿最坏能放过两倍的量，这一维要挡
 * 的是「持续打爆上游」而不是精确速率，够用且只花一次 Redis INCR。要换成令牌桶就得把
 * {@code RedisRateLimiter} 那段 Lua 搬过来。
 *
 * <p>窗口默认 60 秒而不是 1 秒，这是实测出来的必要性：一次模型调用本身就要几秒到几十秒，按秒开窗
 * 时顺序请求永远落在不同窗口里，限了等于没限（把窗口配额调成 1 连打两次，两次都 200）。
 * 上游账户配额一般也是按分钟给的。
 *
 * <p>Redis 不可达时<b>放行</b>：配额存储挂了就把所有流量挡在门外，等于把 Redis 的可用性乘进网关的
 * 可用性里。这一维本来就是软保护，宁可少限不可错杀；租户维度交给 {@code RequestRateLimiter}，
 * 它的行为是拒绝，两者不同是有意的。
 */
@Component
public class ProviderQuotaFilter implements GlobalFilter, Ordered {

    /** 键前缀。带秒级窗口后缀，所以键会自然过期，不需要清理任务。 */
    static final String KEY_PREFIX = "ai-gateway:provider:";

    /** Redis 不可达时的哨兵值：正常计数从 1 开始，不会撞上。 */
    private static final long NO_DATA = -1L;

    private final ReactiveStringRedisTemplate redis;

    private final AiGatewayProperties properties;

    /** 窗口按秒切；时钟可注入，测试里要钉住某一秒才能把「第三个必被拒」写成断言。 */
    private final Clock clock;

    public ProviderQuotaFilter(ReactiveStringRedisTemplate redis, AiGatewayProperties properties, Clock clock) {
        this.redis = redis;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public int getOrder() {
        // 晚于 NettyWriteResponseFilter(-1)，才能自己把 429 写出去；早于 RouteToRequestUrlFilter(10000)，
        // 被拒的请求不会碰到上游
        return 100;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        int limit = properties.getRateLimit().getProviderRequestsPerWindow();
        int window = Math.max(1, properties.getRateLimit().getProviderWindowSeconds());
        String provider = RateLimitConfiguration.providerOf(exchange);
        if (limit <= 0 || provider == null) {
            return chain.filter(exchange);
        }
        // 窗口后缀：把当前秒数按 window 秒取整，同一窗口内所有请求落在同一个键上
        long bucket = clock.instant().getEpochSecond() / window;
        String key = KEY_PREFIX + provider + ":" + bucket;
        // 兜底只包这一次 Redis 调用。写成整条链的 onErrorResume(e -> chain.filter(exchange))
        // 会把下游的正常失败也当成「Redis 挂了」再走一遍链——504 就是这么被吞成 200 空体的。
        return redis.opsForValue().increment(key)
                .onErrorResume(redisUnavailable -> Mono.<Long>empty())
                .defaultIfEmpty(NO_DATA)
                .flatMap(count -> decide(exchange, chain, key, provider, limit, window, count));
    }

    private Mono<Void> decide(ServerWebExchange exchange, GatewayFilterChain chain, String key, String provider,
            int limit, int window, long count) {
        if (count == NO_DATA) {
            // Redis 不可达：放行，见类注释里的取舍
            return chain.filter(exchange);
        }
        if (count == 1L) {
            // 窗口键多留一倍时间：用完就该消失，也不会被下一个窗口读到
            redis.expire(key, Duration.ofSeconds(2L * window)).subscribe();
        }
        return count <= limit ? chain.filter(exchange) : deny(exchange, provider, limit, window);
    }

    private Mono<Void> deny(ServerWebExchange exchange, String provider, int limit, int window) {
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        exchange.getResponse().getHeaders().set("X-Provider-RateLimit-Limit", String.valueOf(limit));
        // 这一维自己算 Retry-After：要等到窗口翻篇，不是 RetryAfterFilter 默认的那 1 秒
        long retryAfter = window - clock.instant().getEpochSecond() % window;
        exchange.getResponse().getHeaders().set(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfter));
        byte[] body = ("{\"error\":{\"message\":\"Provider '" + provider
                + "' quota is exhausted, retry shortly.\",\"type\":\"rate_limit_error\",\"param\":null,"
                + "\"code\":\"provider_rate_limit_exceeded\"}}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return exchange.getResponse()
                .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}
