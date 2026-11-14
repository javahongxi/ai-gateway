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
 * R24 端到端：首字节前的请求内重试。单厂商、不配 backup（所以没有熔断改道，重试就是被测机制）。
 * {@code max-attempts=1}：连接级失败时同一请求内再试一次。
 *
 * <p>三条性质各有一发：连接级失败后重试能接住（桩收到 2 次、客户端 200）；5xx 已带响应体<b>不</b>
 * 重试（桩只收到 1 次——这条同时证明内置默认的 {@code series=[SERVER_ERROR]} 被清掉了）；一直断连
 * 时重试有界（1+1=2 次后失败，不会无限敲）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=request-retry",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.retry.max-attempts=1",
        "ai-gateway.retry.first-backoff=10ms",
        "ai-gateway.retry.max-backoff=50ms",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub",
        "ai-gateway.providers.stub.chat-path=/upstream/chat",
        "ai-gateway.providers.stub.key-ref=sk-stub",
        "ai-gateway.providers.stub.connect-timeout=1000",
        "ai-gateway.providers.stub.response-timeout=5000"
})
@Import(AlwaysAllowRateLimiter.class)
class RequestRetryTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.providers.stub.base-url", upstream::baseUrl);
    }

    @BeforeEach
    void connectAndReset() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
    }

    /** 第一发连接级失败、之后正常：重试接住第二发，客户端 200，桩收到 2 次。 */
    @Test
    void retryRecoversAfterAConnectionFailureBeforeFirstByte() {
        upstream.scenario(StubUpstream.Scenario.RESET_ONCE_THEN_ECHO);

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.requests()).as("第一发断连 + 重试一发").isEqualTo(2);
    }

    /** 5xx 已带响应体，不重试（重试会重复计费）：桩只收到 1 次。这条钉住 series 被清空。 */
    @Test
    void serverErrorWithABodyIsNotRetried() {
        upstream.scenario(StubUpstream.Scenario.FAIL_500);

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(upstream.requests()).as("5xx 不触发重试").isEqualTo(1);
    }

    /** 一直断连：重试有界，1+1=2 次后失败，不会无限敲上游。 */
    @Test
    void retriesAreBoundedWhenUpstreamKeepsFailing() {
        upstream.scenario(StubUpstream.Scenario.RESET);

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(upstream.requests()).as("首发 + 一次重试，到此为止").isEqualTo(2);
    }
}