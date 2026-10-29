package org.hongxi.ai.gateway.routing;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R16 的选取与冷却，纯函数级：不起上下文，时钟是钉住的。
 *
 * <p>时钟必须可换——冷却是按时间算的，用系统时钟写"到期后重新可用"这条断言，同一份代码连跑三次
 * 能给出两种结果（与限流用例那条同一个教训）。所以每个场景现造一个固定时刻的池子。
 */
class UpstreamKeyPoolTest {

    private static final Instant T0 = Instant.parse("2026-10-30T00:00:00Z");

    private static AiGatewayProperties propertiesWith(String alias, List<String> keys, Duration cooldown) {
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setBaseUrl("https://example.invalid");
        declared.setChatPath("/v1/chat/completions");
        declared.setKeys(new ArrayList<>(keys));
        if (cooldown != null) {
            declared.setKeyCooldown(cooldown);
        }
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put(alias, declared);
        AiGatewayProperties properties = new AiGatewayProperties();
        properties.setProviders(providers);
        return properties;
    }

    private static UpstreamKeyPool pool(AiGatewayProperties properties, Instant now) {
        return new UpstreamKeyPool(properties, new SimpleMeterRegistry(),
                Clock.fixed(now, ZoneOffset.UTC));
    }

    @Test
    void rotatesAcrossThePoolAndWrapsAround() {
        UpstreamKeyPool subject = pool(propertiesWith("pooled", List.of("sk-aaaa-1111", "sk-bbbb-2222",
                "sk-cccc-3333"), null), T0);

        assertThat(List.of(subject.pick("pooled"), subject.pick("pooled"), subject.pick("pooled"),
                subject.pick("pooled")))
                .containsExactly("sk-aaaa-1111", "sk-bbbb-2222", "sk-cccc-3333", "sk-aaaa-1111");
    }

    @Test
    void singleKeyPoolIsThatKey() {
        UpstreamKeyPool subject = pool(propertiesWith("solo", List.of("sk-only-9999"), null), T0);

        assertThat(subject.pick("solo")).isEqualTo("sk-only-9999");
        assertThat(subject.pick("solo")).isEqualTo("sk-only-9999");
    }

    /** keyRef 单把的旧写法仍然是合法的池子——不能因为引入 keys 就把老配置判死。 */
    @Test
    void legacySingleKeyRefStillWorks() {
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setKeyRef("sk-legacy-7777");
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("legacy", declared);
        AiGatewayProperties properties = new AiGatewayProperties();
        properties.setProviders(providers);

        assertThat(pool(properties, T0).pick("legacy")).isEqualTo("sk-legacy-7777");
    }

    @Test
    void cooledKeyIsSkippedUntilItsCooldownExpires() {
        AiGatewayProperties properties = propertiesWith("pooled",
                List.of("sk-aaaa-1111", "sk-bbbb-2222"), Duration.ofSeconds(30));

        UpstreamKeyPool atZero = pool(properties, T0);
        assertThat(atZero.pick("pooled")).isEqualTo("sk-aaaa-1111");
        atZero.coolDown("pooled", "sk-aaaa-1111");
        assertThat(atZero.isCooling("pooled", "sk-aaaa-1111")).isTrue();
        assertThat(atZero.pick("pooled")).as("冷却中的那把该让路").isEqualTo("sk-bbbb-2222");

        UpstreamKeyPool afterMinute = pool(properties, T0.plusSeconds(61));
        assertThat(afterMinute.isCooling("pooled", "sk-aaaa-1111"))
                .as("过了冷却时间就该重新可用，否则一次 429 会把 key 永久判死")
                .isFalse();
    }

    /** 全部在冷却时照样发：把流量挡在门外换不来上游更快恢复。 */
    @Test
    void allCoolingStillServesRatherThanBlockingTraffic() {
        UpstreamKeyPool subject = pool(propertiesWith("pooled",
                List.of("sk-aaaa-1111", "sk-bbbb-2222"), Duration.ofMinutes(5)), T0);
        subject.coolDown("pooled", "sk-aaaa-1111");
        subject.coolDown("pooled", "sk-bbbb-2222");

        assertThat(subject.pick("pooled")).isIn("sk-aaaa-1111", "sk-bbbb-2222");
    }

    @Test
    void missingCredentialsYieldNoKey() {
        UpstreamKeyPool subject = pool(propertiesWith("empty", List.of(), null), T0);

        assertThat(subject.pick("empty")).as("没凭证就原样放行，让厂商回它的 401").isNull();
        assertThat(subject.pick("no-such-provider")).isNull();
    }

    @Test
    void fingerprintShowsFourCharsAndNeverMore() {
        assertThat(UpstreamKeyPool.fingerprint("sk-prod-abcd1234")).isEqualTo("****1234");
        assertThat(UpstreamKeyPool.fingerprint("ab")).as("短到几乎就是原文的 key 不给任何部分")
                .isEqualTo("****");
        assertThat(UpstreamKeyPool.fingerprint(null)).isEqualTo("none");
        assertThat(UpstreamKeyPool.fingerprint("sk-prod-abcd1234")).doesNotContain("abcd");
    }

    @Test
    void zeroOrNegativeCooldownDisablesCooling() {
        UpstreamKeyPool subject = pool(propertiesWith("pooled",
                List.of("sk-aaaa-1111", "sk-bbbb-2222"), Duration.ZERO), T0);
        subject.coolDown("pooled", "sk-aaaa-1111");

        assertThat(subject.isCooling("pooled", "sk-aaaa-1111")).isFalse();
    }
}
