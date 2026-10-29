package org.hongxi.ai.gateway;

import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.RequestMetricsFilter;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.GatewayCalls.LiveCall;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 一期验收：OpenAI 兼容入口、厂商凭证注入、租户鉴权与隔离，以及响应超时的真实语义。
 *
 * <p>上游是进程内桩（{@link StubUpstream}），测试路由指向它。断言只落在两类事实上：桩收到了
 * 什么（证明 host / path / header 改写与「不改报文」），以及客户端按什么顺序看到什么（证明流式
 * 不被缓冲、超时管到哪一层）。真实厂商冒烟在 {@code ProviderLiveTest}，按每家的环境变量开关。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 测试不连配置中心：路由与租户全部由下面的 DynamicPropertySource 与本 properties 给出
        // 每个测试类一句自己的标记：Spring 的上下文缓存键只看合并后的配置，
        // 不看 @DynamicPropertySource 贡献的值——两个类的光看注解会算同一个上下文，
        // 路由表就被前一个类复用了（实测整类跑时指标断言会飘）。
        "ai-gateway.test-context=chat-flow",
        "spring.cloud.nacos.config.import-check.enabled=false",
        // default-filters 里挂的是真限流（要 Redis、会扣配额），这些用例不测它
        // 主 yml 里的 optional:nacos import 仍会被处理（spring.config.import= 清空不了
        // 那份文档自带的 import），本机 Nacos 上没有 ai-gateway.yaml 时 SCA 会打一条 NPE
        // 堆栈。功能不受 optional: 影响，测试里把这条噪音压掉
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        // 本类要测的是「response-timeout 管不到流中途」，所以把 R11 的看门狗关掉；
        // 它生效时的行为由 UpstreamIdleTimeoutTest 钉
// 本类的路由是自己注入的（见 @DynamicPropertySource），关掉由 ai-gateway.providers
// 声明生成路由那一档，否则生产声明里的兜底路由会抢走「没人认领的模型」这类断言
        "ai-gateway.generated-routes=false",
        "ai-gateway.upstream.idle-timeout=0",
        // 三个测试类各自一个上下文，固定运维端口会互相抢；测试里随机化
        "management.server.port=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub",
        "ai-gateway.tenants.beta.api-key=sk-beta",
        "ai-gateway.tenants.beta.providers[0]=someone-else",
        "ai-gateway.tenants.gamma.api-key=sk-gamma",
        "ai-gateway.tenants.gamma.providers[0]=stub2"
})
@Import(AlwaysAllowRateLimiter.class)
class GatewayChatFlowTest {

    private static final String ROUTE = "spring.cloud.gateway.server.webflux.routes[0]";

    private static final String SECOND_ROUTE = "spring.cloud.gateway.server.webflux.routes[1]";

    private static final String CHAT_PATH = "/v1/chat/completions";

    /** 第二条厂商路由的路径，一期用路径前缀区分厂商。 */
    private static final String SECOND_CHAT_PATH = "/deepseek/v1/chat/completions";

    /** 客户端发给网关的原始请求体，桩会把它原样收回，用来证明报文没被动过。 */
    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}";

    /** 测试路由的响应头预算，单位毫秒。 */
    private static final int RESPONSE_TIMEOUT_MILLIS = 300;

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry registry;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add(ROUTE + ".id", () -> "stub-chat");
        registry.add(ROUTE + ".uri", upstream::baseUrl);
        registry.add(ROUTE + ".predicates[0]", () -> "Path=" + CHAT_PATH);
        registry.add(ROUTE + ".metadata.provider", () -> "stub");
        // 必须是毫秒数字：写成 300ms 会被 Long.parseLong 拒掉并静默回落到全局值
        registry.add(ROUTE + ".metadata.response-timeout", () -> String.valueOf(RESPONSE_TIMEOUT_MILLIS));
        registry.add(ROUTE + ".metadata.connect-timeout", () -> "1000");
        registry.add(ROUTE + ".filters[0]", () -> "SetPath=/upstream/chat");
        registry.add(ROUTE + ".filters[1]", () -> "SetRequestHeader=Authorization,Bearer provider-secret");
        // 第二条厂商路由：同一个桩，不同的路径前缀、provider 与厂商 key
        registry.add(SECOND_ROUTE + ".id", () -> "stub-chat-second");
        registry.add(SECOND_ROUTE + ".uri", upstream::baseUrl);
        registry.add(SECOND_ROUTE + ".predicates[0]", () -> "Path=" + SECOND_CHAT_PATH);
        registry.add(SECOND_ROUTE + ".metadata.provider", () -> "stub2");
        registry.add(SECOND_ROUTE + ".metadata.response-timeout", () -> String.valueOf(RESPONSE_TIMEOUT_MILLIS));
        registry.add(SECOND_ROUTE + ".filters[0]", () -> "SetPath=/upstream/second");
        registry.add(SECOND_ROUTE + ".filters[1]", () -> "SetRequestHeader=Authorization,Bearer second-secret");
    }

    @AfterAll
    static void shutDownUpstream() {
        upstream.close();
    }

    @BeforeEach
    void connectToGateway() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    @Test
    void missingApiKeyIsRejectedBeforeReachingTheProvider() {
        Outcome outcome = calls.call(CHAT_PATH, null);

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(outcome.body()).contains("\"code\":\"invalid_api_key\"");
        assertThat(upstream.requests()).as("没有身份的请求不该碰到厂商").isZero();
    }

    @Test
    void unknownApiKeyIsRejectedWithTheOpenAiErrorEnvelope() {
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-not-a-real-key");

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(outcome.body())
                .as("客户端按 OpenAI 的 error 信封解析，网关不另立约定")
                .contains("\"error\"")
                .contains("\"type\":\"invalid_request_error\"")
                .contains("\"param\":null");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void tenantCannotReachAProviderOutsideItsAllowlist() {
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-beta");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("provider_not_allowed");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void eachProviderRouteCarriesItsOwnPathAndCredential() {
        Outcome outcome = calls.call(SECOND_CHAT_PATH, "Bearer sk-gamma");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).as("第二条路由改写自己的厂商路径").isEqualTo("/upstream/second");
        assertThat(upstream.authorization()).isEqualTo("Bearer second-secret");
        assertThat(upstream.tenant()).isEqualTo("gamma");
    }

    @Test
    void tenantAllowedForOneProviderCannotUseAnotherProvidersRoute() {
        Outcome outcome = calls.call(SECOND_CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("stub2");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void nonStreamCallIsForwardedWithTheProviderCredentialAndAnUnchangedBody() {
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(outcome.body()).contains("chatcmpl-stub").contains("pong");
        assertThat(upstream.method()).as("方法原样透传").isEqualTo("POST");
        assertThat(upstream.path()).as("SetPath 生效，厂商侧路径由路由决定").isEqualTo("/upstream/chat");
        assertThat(upstream.authorization()).as("客户端的网关 key 被换成厂商 key").isEqualTo("Bearer provider-secret");
        assertThat(upstream.tenant()).isEqualTo("alpha");
        assertThat(upstream.body()).as("决策 D1：请求体逐字节原样").isEqualTo(REQUEST_BODY);
        assertThat(upstream.requests()).as("一次调用只发一次，网关没在背后重发").isEqualTo(1);
    }

    @Test
    void streamIsDeliveredIncrementallyAndSurvivesPastTheResponseTimeout() {
        upstream.scenario(StubUpstream.Scenario.SSE_STEADY);
        // 7 个元素 × 80ms ≈ 560ms，明显超过 300ms 的响应头预算

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(GatewayCalls.reassembleDeltas(outcome.body())).isEqualTo(StubUpstream.STEADY_BODY);
        assertThat(outcome.arrivals()).as("分块逐个交付，没有被攒成一整包").hasSizeGreaterThan(3);
        assertThat(outcome.arrivals().get(0)).isLessThan(RESPONSE_TIMEOUT_MILLIS);
        assertThat(outcome.elapsed())
                .as("总时长超过 response-timeout 仍然收完：它管的是响应头，不是整条流")
                .isGreaterThan(RESPONSE_TIMEOUT_MILLIS);
    }

    @Test
    void lateResponseHeadBecomesGatewayTimeout() {
        upstream.scenario(StubUpstream.Scenario.SILENT);
        // 桩 5 秒后才回响应头

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT.value());
        assertThat(outcome.failure())
                .as("504 是网关写出来的正常响应，不是客户端读流失败")
                .isNull();
        assertThat(outcome.elapsed())
                .as("在预算附近就断开，而不是等满上游的 5 秒")
                .isBetween(250L, 2000L);
        assertThat(upstream.requests())
                .as("超时不触发重发：Retry 默认只覆盖 GET，这里也没配它")
                .isEqualTo(1);
    }

    @Test
    void midStreamStallIsNotCutOffByResponseTimeout() {
        upstream.scenario(StubUpstream.Scenario.SSE_THEN_SILENCE);
        // 桩吐一块之后不再说话。这是 R11 存在的理由：本类的 response-timeout 是 300ms，
        // 而流在 1.5 秒后仍然挂着——正文阶段没有任何超时。看门狗生效后的行为见
        // UpstreamIdleTimeoutTest。

        LiveCall live = calls.start(CHAT_PATH, "Bearer sk-alpha");
        live.awaitQuietly(Duration.ofMillis(1500));

        assertThat(live.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(live.body()).as("第一块已经交付给客户端").contains("Hi");
        assertThat(live.terminated()).as("1.5 秒后流仍未结束").isFalse();
        live.cancel();
    }

    @Test
    void everyCallIsCountedPerTenantAndProviderIncludingRejections() {
        // 同一个上下文里多个测试共用 registry，所以断言增量而不是绝对值
        long okBefore = timerCount("alpha", "stub", "200");
        long rejectedBefore = timerCount("anonymous", "stub", "401");

        calls.call(CHAT_PATH, "Bearer sk-alpha");
        calls.call(CHAT_PATH, null);

        awaitCount("alpha", "stub", "200", okBefore + 1);
        assertThat(timerCount("alpha", "stub", "200") - okBefore)
                .as("成功请求按租户 + 厂商 + 状态计数")
                .isEqualTo(1);
        awaitCount("anonymous", "stub", "401", rejectedBefore + 1);
        assertThat(timerCount("anonymous", "stub", "401") - rejectedBefore)
                .as("被鉴权挡掉的请求也要计数，否则「某租户一直拿错 key 打网关」在指标里隐身")
                .isEqualTo(1);
    }

    /**
     * 等到计数长上去为止。指标是在 {@code doFinally} 里记的，而客户端的 {@code block()} 在响应
     * 提交时就返回——两者没有先后保证，直接读会偶发读到旧值（整类跑时飘过，单跑不复现）。
     * 指标本来就是最终一致的，测试就该按最终一致读。
     */
    private void awaitCount(String tenant, String provider, String status, long atLeast) {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline && timerCount(tenant, provider, status) < atLeast) {
            try {
                Thread.sleep(20);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private long timerCount(String tenant, String provider, String status) {
        Timer timer = registry.find(RequestMetricsFilter.REQUEST_METRIC)
                .tag("tenant", tenant)
                .tag("provider", provider)
                .tag("status", status)
                .timer();
        // 一次都没发生过时 timer() 返回 null，不是 0——「before」读的就是这个空档
        return timer == null ? 0 : timer.count();
    }
}
