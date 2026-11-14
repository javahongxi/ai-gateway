package org.hongxi.ai.gateway.ratelimit;

import java.time.Clock;
import java.time.Duration;

import reactor.core.publisher.Mono;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;

import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.observability.ResponseObservationFilter;

/**
 * R23：租户 token 配额的预扣与结算。形状照 LiteLLM 的 {@code parallel_request_limiter}：
 * <b>请求前按 {@code max_tokens} 预扣 TPM，响应后按真实 usage 结算差额</b>。本网关已有 R12 的
 * usage 采集，把"事后采到的数"回灌限流器就是它。
 *
 * <p><b>固定窗口</b>，与厂商维度同形状（D5）：一个窗口一个键 {@code ai-gateway:tokens:tenant:<租户>:<窗口序号>}，
 * 键带窗口后缀会自然过期。预扣 {@code INCRBY}，超限就 {@code INCRBY} 负值退回；结算再 {@code INCRBY}
 * 差额。只花几次 Redis 命令，不上 Lua。
 *
 * <p><b>Redis 不可达放行</b>（D5/D19）：预扣那一次 {@code onErrorResume} 退回一个"不记账"的预约，
 * 过滤器据此不写属性、结算也就什么都不做；结算本身 fire-and-forget。把配额存储的可用性乘进网关
 * 可用性不值当——这一维是软保护。
 *
 * <p>只做<b>租户 TPM</b>。厂商 TPM 与 prompt 的 tokenizer 精确预扣是后续项：机制可复用，加一维只是
 * 多一个键前缀（决策 D19）。
 */
@Component
public class TokenQuotaService {

    /** 键前缀。带窗口序号后缀，键自然过期，不需要清理任务。 */
    static final String KEY_PREFIX = "ai-gateway:tokens:tenant:";

    /** Redis 不可达时的哨兵：正常累计从预扣额（>0）起，不会撞上。 */
    private static final long NO_DATA = -1L;

    private final ReactiveStringRedisTemplate redis;

    private final AiGatewayProperties properties;

    /** 窗口按秒切；时钟可注入，测试钉住某一秒才能把"第几发必被拒"写成不会偶然通过的断言。 */
    private final Clock clock;

    public TokenQuotaService(ReactiveStringRedisTemplate redis, AiGatewayProperties properties, Clock clock) {
        this.redis = redis;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * 一次预扣的结果。{@code allowed} 决定放不放行；{@code tracked} 决定要不要写属性、结算要不要记账。
     * Redis 挂了或被配额 0 关掉时 {@code tracked=false}——放行但不记账（fail-open）。
     */
    public record Reservation(boolean allowed, boolean tracked, String windowKey, long reserved, long limit) {

        static Reservation pass() {
            return new Reservation(true, false, null, 0L, 0L);
        }

        static Reservation denied(long limit) {
            return new Reservation(false, false, null, 0L, limit);
        }

        static Reservation granted(String windowKey, long reserved, long limit) {
            return new Reservation(true, true, windowKey, reserved, limit);
        }
    }

    public long limit() {
        return properties.getRateLimit().getTenantTokensPerWindow();
    }

    public int windowSeconds() {
        return Math.max(1, properties.getRateLimit().getTokenWindowSeconds());
    }

    /** 窗口剩余秒数，给 429 的 Retry-After 用：要等窗口翻篇，不是默认那 1 秒。 */
    public long retryAfterSeconds() {
        int window = windowSeconds();
        return window - clock.instant().getEpochSecond() % window;
    }

    String windowKey(String tenant) {
        long bucket = clock.instant().getEpochSecond() / windowSeconds();
        return KEY_PREFIX + tenant + ":" + bucket;
    }

    /**
     * 预扣 {@code amount} 个 token。超限退回并返回 {@code denied}；Redis 不可达返回 {@code pass}
     * （放行不记账）。兜底只包这一次 Redis 调用，不包整条链——把下游正常失败当成"Redis 挂了"
     * 再走一遍链会吞掉真实错误（ProviderQuotaFilter 那条 504 被吞成 200 的教训）。
     */
    public Mono<Reservation> reserve(String tenant, long amount) {
        long limit = limit();
        if (limit <= 0 || amount <= 0) {
            return Mono.just(Reservation.pass());
        }
        String key = windowKey(tenant);
        return redis.opsForValue().increment(key, amount)
                .onErrorResume(redisUnavailable -> Mono.<Long>empty())
                .defaultIfEmpty(NO_DATA)
                .flatMap(count -> decide(key, amount, limit, count));
    }

    private Mono<Reservation> decide(String key, long amount, long limit, long count) {
        if (count == NO_DATA) {
            return Mono.just(Reservation.pass());
        }
        if (count == amount) {
            // 这一发开了窗：键多留一倍时间，用完就该消失，也不会被下一个窗口读到
            redis.expire(key, Duration.ofSeconds(2L * windowSeconds())).subscribe();
        }
        if (count > limit) {
            // 超限：把刚预扣的退回，再拒。退回也包一层兜底，Redis 半死不活时不该把拒绝卡住
            return redis.opsForValue().increment(key, -amount)
                    .onErrorResume(redisUnavailable -> Mono.<Long>empty())
                    .thenReturn(Reservation.denied(limit));
        }
        return Mono.just(Reservation.granted(key, amount, limit));
    }

    /**
     * 结算（响应阶段，由 {@code ResponseObservationFilter} 调）。预约是请求阶段写进属性的，外层读得到
     * （D11）。三种处置：
     * <ul>
     *   <li>非 2xx：全额退预扣——这次调用没在厂商产出计费 completion；</li>
     *   <li>2xx 且有 usage：{@code INCRBY (prompt+completion − 预扣)}，cached/reasoning 是子集不重复计；</li>
     *   <li>2xx 但无 usage：保留预扣不退——"发生了但量未知"，宁可多限不可少限。</li>
     * </ul>
     * fire-and-forget：结算失败不该影响已经发给客户端的响应。
     */
    public void settle(Reservation reservation, boolean successfulHttp, ResponseObservationFilter.Usage usage) {
        if (reservation == null || !reservation.tracked()) {
            return;
        }
        long delta;
        if (!successfulHttp) {
            delta = -reservation.reserved();
        }
        else if (usage != null) {
            delta = usage.promptTokens() + usage.completionTokens() - reservation.reserved();
        }
        else {
            return;
        }
        if (delta == 0) {
            return;
        }
        redis.opsForValue().increment(reservation.windowKey(), delta)
                .onErrorResume(redisUnavailable -> Mono.empty())
                .subscribe();
    }
}