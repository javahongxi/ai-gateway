package org.hongxi.ai.gateway.routing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.stereotype.Component;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R16：一家厂商多把上游 key 时的选取与冷却。
 *
 * <p>为什么需要它：上游给的是**按 key** 的账户配额。只按厂商计窗口，两把 key 会互相顶包——A 把
 * 配额用光了，B 还被算成"已经用满"。所以两件事分开：窗口按 {@code provider:key} 分桶（见
 * {@code ProviderQuotaFilter}），被限流的那把 key 单独进冷却。
 *
 * <p>策略是<b>跳过冷却中的、其余轮转</b>，不是"选最闲的那把"：闲不闲只有上游知道，网关拿到的信号
 * 只有它回没回 429。全部 key 都在冷却时<em>照样发</em>并记一次 {@code all-cooling}——把流量挡在
 * 门外换不来上游更快恢复，这条与厂商限流"Redis 挂了也放行"是同一个判断。
 *
 * <p>key 的原文不出这个类：对外一律 {@link #fingerprint}（末四位）。日志、指标、异常信息里出现
 * 完整上游凭证是最常见的泄漏方式之一。
 */
@Component
public class UpstreamKeyPool {

    /** 本次调用选中 key 的指纹，给日志与厂商维度窗口用（请求阶段写，外层读得到）。 */
    public static final String SELECTED_FINGERPRINT_ATTRIBUTE = "aiGatewayUpstreamKey";

    /** 本次调用选中的 key 原文，只用于冷却登记，绝不进日志。 */
    public static final String SELECTED_KEY_ATTRIBUTE = "aiGatewayUpstreamRawKey";

    /** 选取分布：{@code rotated|single|all-cooling|empty}。 */
    public static final String SELECT_METRIC = "ai.gateway.provider.keys";

    private final AiGatewayProperties properties;

    private final MeterRegistry registry;

    /** 时钟可注入：冷却是按时间算的，测试要钉住某一秒才能把"到期后重新可用"写成不会偶然通过的断言。 */
    private final Clock clock;

    private final Map<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

    /** 冷却到期时刻，按 {@code provider|指纹} 存。 */
    private final Map<String, Instant> coolingUntil = new ConcurrentHashMap<>();

    public UpstreamKeyPool(AiGatewayProperties properties, MeterRegistry registry, Clock clock) {
        this.properties = properties;
        this.registry = registry;
        this.clock = clock;
    }

    /**
     * 取一把 key。返回 {@code null} 表示这家厂商没有可用凭证（配置漏了），此时调用方应当把请求
     * 原样发出去让厂商回 401——那是最容易被读到的信号，比网关自己编一个错误码更接近事实。
     */
    public String pick(String provider) {
        AiGatewayProperties.Provider declared = properties.getProviders().get(provider);
        if (declared == null) {
            return null;
        }
        List<String> keys = declared.effectiveKeys();
        if (keys.isEmpty()) {
            select("empty", provider);
            return null;
        }
        if (keys.size() == 1) {
            select("single", provider);
            return keys.get(0);
        }

        int size = keys.size();
        int start = cursors.computeIfAbsent(provider, key -> new AtomicInteger()).getAndIncrement();
        for (int offset = 0; offset < size; offset++) {
            String candidate = keys.get(Math.floorMod(start + offset, size));
            if (!isCooling(provider, candidate)) {
                select("rotated", provider);
                return candidate;
            }
        }
        // 全在冷却：照发，不挡流量
        select("all-cooling", provider);
        return keys.get(Math.floorMod(start, size));
    }

    /** 把一把 key 关进冷却（上游回了 429，或正文里是自报限流）。 */
    public void coolDown(String provider, String rawKey) {
        AiGatewayProperties.Provider declared = properties.getProviders().get(provider);
        Duration cooldown = declared == null ? null : declared.getKeyCooldown();
        if (cooldown == null || cooldown.isZero() || cooldown.isNegative()) {
            return;
        }
        coolingUntil.put(slot(provider, rawKey), clock.instant().plus(cooldown));
    }

    public boolean isCooling(String provider, String rawKey) {
        Instant until = coolingUntil.get(slot(provider, rawKey));
        if (until == null) {
            return false;
        }
        if (until.isAfter(clock.instant())) {
            return true;
        }
        // 到期就顺手清掉，不让这张表只增不减
        coolingUntil.remove(slot(provider, rawKey), until);
        return false;
    }

    /** 末四位；短于四位的 key 不给任何部分，避免"指纹"其实接近原文。 */
    public static String fingerprint(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return "none";
        }
        String trimmed = rawKey.trim();
        if (trimmed.length() <= 4) {
            return "****";
        }
        return "****" + trimmed.substring(trimmed.length() - 4);
    }

    private String slot(String provider, String rawKey) {
        return provider + "|" + fingerprint(rawKey);
    }

    private void select(String outcome, String provider) {
        Counter.builder(SELECT_METRIC)
                .description("上游 key 的选取结果分布")
                .tag("outcome", outcome)
                .tag("provider", provider)
                .register(registry)
                .increment();
    }
}
