package org.hongxi.ai.gateway.config;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R17 的租户判定语义，纯配置级：白名单为空等于不限制、模型名走 glob、过期是"到点即失效"。
 *
 * <p>值得单独钉的是白名单非空却<b>读不到模型名</b>那一格：按不允许处理。放开等于让客户端用畸形
 * 请求体绕过白名单，而"绕过"比"被拒"难查得多。
 */
class TenantGovernanceTest {

    private static AiGatewayProperties.Tenant tenant(List<String> models, Instant expiresAt) {
        AiGatewayProperties.Tenant tenant = new AiGatewayProperties.Tenant();
        tenant.setApiKey("sk-test");
        tenant.setAllowedModels(models);
        tenant.setExpiresAt(expiresAt);
        return tenant;
    }

    @Test
    void emptyAllowlistMeansAnyModel() {
        assertThat(tenant(List.of(), null).modelAllowed("anything")).isTrue();
        assertThat(tenant(null, null).modelAllowed(null)).isTrue();
    }

    @Test
    void globMatchesTheWayTheModelPredicateDoes() {
        AiGatewayProperties.Tenant gated = tenant(List.of("qwen*", "deepseek-v4-flash"), null);

        assertThat(gated.modelAllowed("qwen3.7-plus")).isTrue();
        assertThat(gated.modelAllowed("deepseek-v4-flash")).isTrue();
        assertThat(gated.modelAllowed("glm-4-plus")).isFalse();
        assertThat(gated.modelAllowed("QWEN3.7-plus")).as("模型名大小写敏感，别悄悄放宽").isFalse();
    }

    @Test
    void missingModelIsRefusedWhenAnAllowlistExists() {
        assertThat(tenant(List.of("qwen*"), null).modelAllowed(null)).isFalse();
        assertThat(tenant(List.of("qwen*"), null).modelAllowed("  ")).isFalse();
    }

    @Test
    void expiryIsInclusiveAtTheInstant() {
        Instant deadline = Instant.parse("2026-01-01T00:00:00Z");
        AiGatewayProperties.Tenant tenant = tenant(List.of(), deadline);

        assertThat(tenant.expiredAt(deadline.minusSeconds(1))).isFalse();
        assertThat(tenant.expiredAt(deadline)).as("到点即失效，不留模糊区").isTrue();
        assertThat(tenant.expiredAt(deadline.plusSeconds(1))).isTrue();
        assertThat(tenant(List.of(), null).expiredAt(Instant.now())).as("没写 expires-at 就是不过期").isFalse();
    }

    @Test
    void providerIsolationStillWorksAlongsideTheNewGates() {
        AiGatewayProperties.Tenant tenant = tenant(List.of(), null);
        tenant.setProviders(List.of("dashscope"));

        assertThat(tenant.allows("dashscope")).isTrue();
        assertThat(tenant.allows("deepseek")).isFalse();
    }
}
