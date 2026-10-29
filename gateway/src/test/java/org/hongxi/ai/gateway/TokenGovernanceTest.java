package org.hongxi.ai.gateway;

import java.nio.charset.StandardCharsets;

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
 * R17 的三道闸门走一遍：模型白名单、来源地址、令牌到期。三条都必须在<b>碰到厂商之前</b>生效，
 * 所以每条都断言桩上的请求数为 0。
 *
 * <p>还有一条与"读请求体做判断"直接相关的：白名单命中之后，交给厂商的正文必须与客户端发出的
 * 逐字节相同。网关为了看一个字段而重排 JSON，是决策 D1 最隐蔽的破法——这里钉住它。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=token-governance",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.generated-routes=false",
        "spring.cloud.gateway.server.webflux.routes[0].id=gated-chat",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=gated",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0]=SetPath=/upstream/gated",
        "ai-gateway.providers.gated.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.gated.chat-path=/upstream/gated",
        "ai-gateway.providers.gated.key-ref=gated-secret",
        // 只许 qwen*；其余一律 403
        "ai-gateway.tenants.gated.api-key=sk-gated",
        "ai-gateway.tenants.gated.providers[0]=gated",
        "ai-gateway.tenants.gated.allowed-models[0]=qwen*",
        // 无任何限制的一组，用来确认新闸门没有把老行为一起锁上
        "ai-gateway.tenants.open.api-key=sk-open",
        "ai-gateway.tenants.open.providers[0]=gated",
        // 本机回环不在这个段里，必然被拒
        "ai-gateway.tenants.iplocked.api-key=sk-iplocked",
        "ai-gateway.tenants.iplocked.providers[0]=gated",
        "ai-gateway.tenants.iplocked.allow-ips[0]=10.0.0.0/8",
        // 2020 年就过期了
        "ai-gateway.tenants.expired.api-key=sk-expired",
        "ai-gateway.tenants.expired.providers[0]=gated",
        "ai-gateway.tenants.expired.expires-at=2020-01-01T00:00:00Z"
})
@Import(AlwaysAllowRateLimiter.class)
class TokenGovernanceTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @BeforeEach
    void startClean() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, null);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    private Outcome chat(String apiKey, String body) {
        return calls.call(CHAT_PATH, "Bearer " + apiKey, body);
    }

    @Test
    void allowedModelReachesTheProviderByteForByte() {
        String body = "{\"model\":\"qwen3.7-plus\",\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}],"
                + "\"temperature\":0.7,\"stream\":true}";

        Outcome outcome = chat("sk-gated", body);

        assertThat(outcome.status())
                .as("白名单命中就该放行（响应体：" + outcome.body() + "）")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/gated")).isEqualTo(1);
        assertThat(upstream.body())
                .as("为了看一个字段而重排 JSON 就是改了报文：字段顺序、空白、都要原样")
                .isEqualTo(body);
    }

    @Test
    void unlistedModelIsRefusedBeforeReachingTheProvider() {
        Outcome outcome = chat("sk-gated", "{\"model\":\"glm-4-plus\",\"messages\":[]}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("model_not_allowed");
        assertThat(upstream.requests()).as("被拒的请求不该敲厂商").isZero();
    }

    /** 读不到模型名的请求体（非 JSON、缺字段）在有白名单时按不允许处理——否则等于绕过。 */
    @Test
    void unparsableBodyCannotSlipPastTheAllowlist() {
        Outcome notJson = chat("sk-gated", "hello");
        Outcome noModel = chat("sk-gated", "{\"messages\":[]}");

        assertThat(notJson.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(notJson.body()).contains("model_not_allowed");
        assertThat(noModel.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(upstream.requests()).isZero();
    }

    /** 没有配白名单的租户行为不变，而且 GET 这类无 JSON 正文的请求不被拖去读体。 */
    @Test
    void tenantsWithoutTheGatesKeepWorking() {
        Outcome ok = chat("sk-open", "{\"model\":\"anything-at-all\",\"messages\":[]}");

        assertThat(ok.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.body()).doesNotContain("model_not_allowed");
        assertThat(new String(upstream.body().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))
                .contains("anything-at-all");
    }

    @Test
    void sourceOutsideTheAllowedRangeIsRefused() {
        Outcome outcome = chat("sk-iplocked", "{\"model\":\"qwen3.7-plus\",\"messages\":[]}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("source_ip_not_allowed");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void expiredTokenIsRejectedLikeARevokedOne() {
        Outcome outcome = chat("sk-expired", "{\"model\":\"qwen3.7-plus\",\"messages\":[]}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(outcome.body()).contains("api_key_expired");
        assertThat(upstream.requests()).isZero();
    }
}
