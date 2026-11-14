package org.hongxi.ai.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R22 端到端：{@code GET /v1/models} 真的被生成器装配成一条 forward 路由，清单从
 * providers/model-groups 聚合而来，而且<b>鉴权不绕过</b>（决策 D18）——带合法租户 key 才读得到，
 * 不带 key 与 chat 入口同一套 401 信封。
 *
 * <p>清单路由不碰上游，所以这里不需要桩真的回话；桩只是给厂商声明一个可达的 base-url，
 * 让生成器愿意产出路由。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=models-list",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=deepseek",
        "ai-gateway.providers.deepseek.chat-path=/upstream/chat",
        "ai-gateway.providers.deepseek.key-ref=sk-deepseek",
        "ai-gateway.providers.deepseek.models[0]=deepseek*",
        "ai-gateway.providers.deepseek.model-mapping.deepseek-chat=deepseek-v4",
        "ai-gateway.model-groups.chat-default[0].provider=deepseek",
        "ai-gateway.model-groups.chat-default[0].model=deepseek-v4"
})
@Import(AlwaysAllowRateLimiter.class)
class ModelsListTest {

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.providers.deepseek.base-url", upstream::baseUrl);
    }

    @BeforeEach
    void connect() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, null);
        upstream.reset();
    }

    @Test
    void authenticatedClientGetsTheAggregatedCatalog() {
        Outcome outcome = calls.get("/v1/models", "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(outcome.body()).contains("\"object\":\"list\"");
        // 组名与各厂商映射键都在；通配模式 deepseek* 不在
        assertThat(outcome.body()).contains("\"id\":\"chat-default\"");
        assertThat(outcome.body()).contains("\"id\":\"deepseek-chat\"");
        assertThat(outcome.body()).contains("\"owned_by\":\"ai-gateway\"");
        assertThat(outcome.body()).contains("\"owned_by\":\"deepseek\"");
        assertThat(outcome.body()).doesNotContain("deepseek*");
        assertThat(upstream.requests()).as("清单是网关本地聚合，不透传厂商").isZero();
    }

    /** 不带 key 与 chat 入口同一套 401 信封：清单不是匿名可读的（D18）。 */
    @Test
    void anonymousClientIsRejectedWithTheSameErrorEnvelope() {
        Outcome outcome = calls.get("/v1/models", null);

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(outcome.body()).contains("invalid_api_key");
        assertThat(upstream.requests()).isZero();
    }

    /** 清单只认 GET：POST 不该命中这条路由（Method 谓词）。 */
    @Test
    void postToModelsEntryDoesNotMatchTheCatalogRoute() {
        Outcome outcome = calls.call("/v1/models", "Bearer sk-alpha", "{}");

        assertThat(outcome.status()).isNotEqualTo(HttpStatus.OK.value());
    }
}