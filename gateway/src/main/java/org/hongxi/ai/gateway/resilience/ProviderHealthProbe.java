package org.hongxi.ai.gateway.resilience;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.http.HttpHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R15：给已经打开熔断的厂商做一次轻量探活，厂商恢复就<b>主动</b>收闸。
 *
 * <p><b>为什么需要主动探。</b>熔断半开只在"下一个真实请求到来"时才放行探测，而打开状态下默认
 * 要等满 {@code wait-duration-in-open-state}。这段时间里厂商其实早就恢复了，真实流量却还被绕开——
 * 用户视角看就是"好好的模型怎么十分钟不给我用"。用 {@code /v1/models} 这类端点探一下，几块钱的
 * 代价换回一个不依赖真实流量的恢复路径。
 *
 * <p><b>只治不杀。</b>探到 2xx 才 {@code transitionToClosedState()}；探到非 2xx 只记指标，不改
 * 状态。判死是熔断自己的事（真实调用的错误率、R14 的连续语义命中），这里再插一脚只会让两个
 * 观察者抢同一个状态机。
 *
 * <p><b>不碰业务指标。</b>探测走自己的 {@link WebClient}，完全绕开网关过滤器链，所以它不会进
 * {@code ai.gateway.requests} 的分位数、不产生 usage、也不落访问日志那一行——否则一次探活会被
 * 当成一次模型调用统计进去，看板上"厂商恢复了"的那一刻会莫名其妙出现三个成功请求。
 *
 * <p>没配 {@code health-path} 的厂商直接跳过（记 {@code outcome=skipped}）：不猜端点，因为各家
 * 模型列表路径确实不一样，猜错的探测比没有探测更糟。
 */
@Component
public class ProviderHealthProbe {

    /** 探测次数，按结果分：{@code healed|unhealthy|skipped|already-closed}。 */
    public static final String PROBE_METRIC = "ai.gateway.provider.probes";

    /** 最近一次探测的结论（1=厂商活着），按厂商分条。 */
    public static final String HEALTHY_GAUGE = "ai.gateway.provider.healthy";

    private static final Logger log = LoggerFactory.getLogger(ProviderHealthProbe.class);

    private final AiGatewayProperties properties;

    private final CircuitBreakerRegistry circuitBreakers;

    private final MeterRegistry registry;

    /** 每个厂商一份健康位，gauge 绑在它上面；缺省 0（未知当不健康，看板才不会被老数据骗）。 */
    private final Map<String, AtomicInteger> health = new ConcurrentHashMap<>();

    public ProviderHealthProbe(AiGatewayProperties properties, CircuitBreakerRegistry circuitBreakers,
            MeterRegistry registry) {
        this.properties = properties;
        this.circuitBreakers = circuitBreakers;
        this.registry = registry;
    }

    /**
     * 定时入口。初始延迟等于间隔：否则调度器一启动就打一轮，测试和刚发布的应用会凭空多出几次
     * 外部调用。关掉探测（{@code ai-gateway.probe.enabled=false}）时这里什么都不做。
     */
    @Scheduled(fixedDelayString = "${ai-gateway.probe.interval:30s}",
            initialDelayString = "${ai-gateway.probe.interval:30s}")
    public void scheduledProbe() {
        if (properties.getProbe().isEnabled()) {
            probeAll();
        }
    }

    /**
     * 探一轮，返回处理过的厂商数。公开是给测试和运维脚本用的：断言"探到 2xx 就收闸"这件事不该靠
     * 睡眠与真实时间参与，直接调它就行。
     */
    public int probeAll() {
        int handled = 0;
        for (Map.Entry<String, AiGatewayProperties.Provider> entry : properties.getProviders().entrySet()) {
            String alias = entry.getKey();
            AiGatewayProperties.Provider provider = entry.getValue();
            CircuitBreaker breaker = circuitBreakers.find(alias).orElse(null);
            if (breaker == null) {
                // 没有熔断器的厂商（没配 backup，或者压根是手写路由）不需要恢复：它从没被判死过
                continue;
            }
            handled++;
            probeOne(alias, provider, breaker);
        }
        return handled;
    }

    private void probeOne(String alias, AiGatewayProperties.Provider provider, CircuitBreaker breaker) {
        if (breaker.getState() == CircuitBreaker.State.CLOSED) {
            // 活着就不敲。这是本功能全部外部流量的来源，省着用
            outcome("already-closed", alias);
            return;
        }
        String path = provider.getHealthPath();
        if (path == null || path.isBlank() || provider.getBaseUrl() == null || provider.getBaseUrl().isBlank()) {
            outcome("skipped", alias);
            log.debug("厂商 '{}' 没有配 health-path，跳过探活（不猜端点）", alias);
            return;
        }

        try {
            int status = WebClient.create(provider.getBaseUrl())
                    .get()
                    .uri(path)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + provider.getKeyRef())
                    .exchangeToMono(response -> Mono.just(response.statusCode().value()))
                    .block(properties.getProbe().getTimeout());
            if (status >= 200 && status < 300) {
                healthOf(alias).set(1);
                outcome("healed", alias);
                if (breaker.getState() != CircuitBreaker.State.CLOSED) {
                    breaker.transitionToClosedState();
                    log.info("厂商 '{}' 探活返回 {}，熔断收闸", alias, status);
                }
            }
            else {
                healthOf(alias).set(0);
                outcome("unhealthy", alias);
                // 只记，不动状态：判死归熔断自己管（见类注释）
                log.debug("厂商 '{}' 探活返回 {}，保持现状", alias, status);
            }
        }
        catch (Exception unreachable) {
            healthOf(alias).set(0);
            outcome("unhealthy", alias);
            log.debug("厂商 '{}' 探活失败：{}", alias, unreachable.toString());
        }
    }

    private AtomicInteger healthOf(String alias) {
        return health.computeIfAbsent(alias, key -> {
            AtomicInteger value = new AtomicInteger(0);
            Gauge.builder(HEALTHY_GAUGE, value, AtomicInteger::get)
                    .description("最近一次探活结论，1 表示厂商活着")
                    .tag("provider", key)
                    .register(registry);
            return value;
        });
    }

    private void outcome(String outcome, String alias) {
        Counter.builder(PROBE_METRIC)
                .description("探活结果分布")
                .tag("outcome", outcome)
                .tag("provider", alias)
                .register(registry)
                .increment();
    }
}
