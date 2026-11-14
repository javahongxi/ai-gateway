package org.hongxi.ai.gateway.resilience;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.routing.UpstreamKeyPool;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R25 默认关：纯函数级，不起上下文、不连 Redis。{@code enabled=false} 时 {@link SharedState} 的每个
 * 方法都在碰 Redis 之前短路——所以这里故意传一个 <b>null</b> 的 Redis 模板：要是哪个方法没短路、真去
 * 碰了它，立刻 NPE。这条钉住「默认关不引入任何 Redis 耦合」（D21），与现有 {@code UpstreamKeyPoolTest}
 * （3 参纯本地构造，不改仍绿）互为表里。
 */
class SharedStateDisabledTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-30T00:00:00Z"), ZoneOffset.UTC);

    private static AiGatewayProperties disabled() {
        AiGatewayProperties properties = new AiGatewayProperties();
        properties.getSharedState().setEnabled(false);
        return properties;
    }

    /** 默认关时每个方法都短路：传 null redis 也不 NPE，读一律 false。 */
    @Test
    void disabledSharedStateNeverTouchesRedis() {
        SharedState subject = new SharedState(null, disabled(), CLOCK);

        assertThat(subject.enabled()).isFalse();
        subject.markCooling("p", "****1111", Duration.ofSeconds(30));
        subject.markOpen("p", Duration.ofSeconds(30));
        subject.markClosed("p");
        assertThat(subject.isCooling("p", "****1111")).isFalse();
        assertThat(subject.isOpen("p")).isFalse();
    }

    /** 默认关时冷却只在本 JVM 内可见：A 冷却某 key，B 看不到——与引入共享前的现状逐条一致。 */
    @Test
    void disabledPoolKeepsCooldownLocalOnly() {
        AiGatewayProperties properties = disabled();
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setKeys(new ArrayList<>(List.of("sk-aaaa-1111", "sk-bbbb-2222")));
        declared.setKeyCooldown(Duration.ofSeconds(30));
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("pooled", declared);
        properties.setProviders(providers);

        UpstreamKeyPool a = new UpstreamKeyPool(properties, new SimpleMeterRegistry(), CLOCK,
                new SharedState(null, properties, CLOCK));
        UpstreamKeyPool b = new UpstreamKeyPool(properties, new SimpleMeterRegistry(), CLOCK,
                new SharedState(null, properties, CLOCK));

        a.coolDown("pooled", "sk-aaaa-1111");

        assertThat(a.isCooling("pooled", "sk-aaaa-1111")).as("本实例本地冷却照常").isTrue();
        assertThat(b.isCooling("pooled", "sk-aaaa-1111")).as("默认关：冷却不跨实例").isFalse();
    }
}