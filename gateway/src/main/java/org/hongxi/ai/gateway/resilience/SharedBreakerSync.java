package org.hongxi.ai.gateway.resilience;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R25：熔断开闸状态的跨实例共享。一台状态机只能有一个写者（D12 的教训），所以本类是远端开闸信号
 * 与本地熔断之间的<b>唯一</b>桥梁，分两半：
 *
 * <ul>
 *   <li><b>写</b>：给注册表挂 {@code onEntryAdded → onStateTransition} 监听。本地熔断 →OPEN 时写远端
 *   标记（TTL=wait-duration-in-open-state），→CLOSED 时删。别的实例据此知道这家被判死了。</li>
 *   <li><b>读</b>：{@link #syncOpenBreakers()} 定时扫各厂商的远端标记，标记在而本地熔断非 OPEN 就
 *   {@code transitionToOpenState()}。<b>只开不关</b>——收闸仍归本地探活（R15/D13）与半开，远端信号
 *   绝不触发收闸，否则两个观察者抢同一台状态机。</li>
 * </ul>
 *
 * <p>默认关（{@code shared-state.enabled=false}）时不注册监听、同步入口直接返回，熔断完全是本地的事，
 * 现有行为不变。同步至多滞后一个 {@code sync-interval}，这是比实时 pub/sub 简单换来的代价（D21）。
 */
@Component
public class SharedBreakerSync {

    private static final Logger log = LoggerFactory.getLogger(SharedBreakerSync.class);

    /** 取不到 wait-duration 时的兜底 TTL，与 application.yml 的默认开闸等待对齐。 */
    private static final Duration FALLBACK_WAIT = Duration.ofSeconds(30);

    private final CircuitBreakerRegistry circuitBreakers;

    private final SharedState sharedState;

    private final AiGatewayProperties properties;

    public SharedBreakerSync(CircuitBreakerRegistry circuitBreakers, SharedState sharedState,
            AiGatewayProperties properties) {
        this.circuitBreakers = circuitBreakers;
        this.sharedState = sharedState;
        this.properties = properties;
    }

    /**
     * 启动时挂监听。既覆盖启动后才懒创建的熔断（onEntryAdded），也覆盖此刻已存在的（逐个挂），
     * 免得「先建后挂」漏掉一台。默认关时什么都不挂。
     */
    @PostConstruct
    void registerListener() {
        if (!sharedState.enabled()) {
            return;
        }
        circuitBreakers.getAllCircuitBreakers().forEach(this::attach);
        circuitBreakers.getEventPublisher().onEntryAdded(entry -> attach(entry.getAddedEntry()));
    }

    private void attach(CircuitBreaker breaker) {
        breaker.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State to = event.getStateTransition().getToState();
            if (to == CircuitBreaker.State.OPEN) {
                sharedState.markOpen(breaker.getName(), waitDuration(breaker));
            }
            else if (to == CircuitBreaker.State.CLOSED) {
                sharedState.markClosed(breaker.getName());
            }
            // HALF_OPEN/DISABLED 等不写远端：开闸信号只认 OPEN，收闸只认 CLOSED
        });
    }

    /**
     * 定时入口。初始延迟等于间隔：否则调度器一启动就打一轮，测试与刚发布的应用会凭空多读一次 Redis。
     * 公开是给测试与运维脚本用的——断言「远端标记在就开本地闸」不该靠睡眠与真实时间参与，直接调它就行。
     */
    @Scheduled(fixedDelayString = "${ai-gateway.shared-state.sync-interval:5s}",
            initialDelayString = "${ai-gateway.shared-state.sync-interval:5s}")
    public void syncOpenBreakers() {
        if (!sharedState.enabled()) {
            return;
        }
        for (String alias : properties.getProviders().keySet()) {
            // 只动已经存在的熔断：没配 backup 的厂商压根没有熔断器，给它凭空建一台再打开没有意义
            CircuitBreaker breaker = circuitBreakers.find(alias).orElse(null);
            if (breaker == null || breaker.getState() == CircuitBreaker.State.OPEN) {
                continue;
            }
            if (sharedState.isOpen(alias)) {
                breaker.transitionToOpenState();
                log.info("厂商 '{}' 远端开闸标记在，本地熔断同步打开（只开不关，收闸归探活/半开）", alias);
            }
        }
    }

    /**
     * 开闸标记的 TTL 取本地熔断的 wait-duration-in-open-state：标记活得和「这家被判死后要等多久」
     * 一样长，到期自然消失。resilience4j 2.x 用 {@code IntervalFunction} 表达等待，取首项即基准时长。
     */
    private static Duration waitDuration(CircuitBreaker breaker) {
        Long millis = breaker.getCircuitBreakerConfig().getWaitIntervalFunctionInOpenState().apply(1);
        return millis == null || millis <= 0 ? FALLBACK_WAIT : Duration.ofMillis(millis);
    }
}