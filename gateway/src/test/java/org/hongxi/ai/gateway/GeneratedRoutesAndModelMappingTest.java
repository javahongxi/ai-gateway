package org.hongxi.ai.gateway;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
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
 * 第六期端到端：{@code ai-gateway.providers} 声明真的被 spring-cloud-gateway 装配成可匹配的路由，
 * 并且 D9 的请求侧模型名改写在三条路径上都成立——直接命中、点名厂商、以及<b>改道之后</b>。
 *
 * <p>拓扑是一个连不上的主厂商（{@code dead}，认领 {@code chat-default}）加一个活着的备用厂商
 * （{@code alive}，兜底）。两边对同一个客户端名字各有各的真名，所以「改道后送到备用厂商的还是
 * 主厂商的名字」这个 D6.4 的坑在这里会被直接证伪或证实。
 *
 * <p>另一个方向也要防住：映射表里没写的名字必须<b>逐字节原样</b>送出去。改写这件事一旦做成
 * 「无论如何都重新序列化一遍」，今天那条「网关不改报文」的断言就悄悄失效了。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 上下文缓存键只看合并后的配置，不看 @DynamicPropertySource 的值——每个类留一句自己的标记
        "ai-gateway.test-context=generated-routes",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        // 熔断计数窗口小一点，让「主厂商连不上」两次就能看出改道行为
        "resilience4j.circuitbreaker.configs.default.sliding-window-size=2",
        "resilience4j.circuitbreaker.configs.default.minimum-number-of-calls=2",
        "resilience4j.circuitbreaker.configs.default.failure-rate-threshold=50",
        "resilience4j.circuitbreaker.configs.default.wait-duration-in-open-state=60s",
        // 与生产一致：TimeLimiter 默认 1 秒会把模型调用全判成超时（D6.2）
        "resilience4j.timelimiter.configs.default.timeout-duration=120s",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=dead",
        "ai-gateway.tenants.alpha.providers[1]=alive",
        // 主厂商：认领 chat-default，但端口没人听；备用是 alive
        "ai-gateway.providers.dead.chat-path=/upstream/dead",
        "ai-gateway.providers.dead.key-ref=dead-secret",
        "ai-gateway.providers.dead.models[0]=chat-default",
        "ai-gateway.providers.dead.models[1]=dead*",
        "ai-gateway.providers.dead.backup=alive",
        "ai-gateway.providers.dead.connect-timeout=200",
        "ai-gateway.providers.dead.response-timeout=5000",
        "ai-gateway.providers.dead.model-mapping.chat-default=dead-real",
        // 备用厂商：兜底（不认领模型），自己的映射表把同一个客户端名字换成另一个真名
        "ai-gateway.providers.alive.chat-path=/upstream/alive",
        "ai-gateway.providers.alive.key-ref=alive-secret",
        "ai-gateway.providers.alive.model-mapping.chat-default=alive-real",
        // 逃生舱：一条手写路由，与生成的兜底路由抢同一个入口路径。生成的带 order=1000，
        // 手写的默认 0，所以手写这条应该赢——D10 卡点①的结论就是这条用例。
        "spring.cloud.gateway.server.webflux.routes[0].id=handwritten-override",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[1]=Model=shadow*",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=alive",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].name=SetPath",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].args.template=/upstream/handwritten",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1].name=SetRequestHeader",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1].args.name=Authorization",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1].args.value=Bearer handwritten-secret"
})
@Import(AlwaysAllowRateLimiter.class)
class GeneratedRoutesAndModelMappingTest {

    private static final String CHAT_ENTRY = "/v1/chat/completions";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        // 主厂商：一个高位没人听的端口。不用 127.0.0.1:1——这台机器上连特权端口拿不到立即 RST，
        // 要等满约 10 秒（实测），那是环境噪声不是被测行为。
        registry.add("ai-gateway.providers.dead.base-url", () -> "http://127.0.0.1:65000");
        registry.add("ai-gateway.providers.alive.base-url", upstream::baseUrl);
        // 手写那条路由的 uri 也得指向同一个桩：占位符在绑定阶段解析，值由这里给
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @BeforeEach
    void resetStub() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, null);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    private Outcome chat(String model) {
        String body = "{\"model\":\"" + model + "\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
        return calls.call(CHAT_ENTRY, "Bearer sk-alpha", body);
    }

    private Outcome chatAt(String path, String model) {
        String body = "{\"model\":\"" + model + "\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
        return calls.call(path, "Bearer sk-alpha", body);
    }

    /** 点名厂商这条路上：客户端名字被换成 alive 那张表里的真名，长度也跟着改对。 */
    @Test
    void namedProviderRouteMapsTheModelAndFixesContentLength() {
        Outcome outcome = chatAt("/alive" + CHAT_ENTRY, "chat-default");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).isEqualTo("/upstream/alive");
        assertThat(upstream.body()).contains("\"model\":\"alive-real\"");
        assertThat(upstream.body())
                .as("客户端传的名字不该原样送给厂商")
                .doesNotContain("chat-default");
        assertThat(Integer.parseInt(upstream.contentLength()))
                .as("改写后字节数变了，Content-Length 必须跟着重算")
                .isEqualTo(upstream.body().getBytes(StandardCharsets.UTF_8).length);
    }

    /** 表里没有的名字原样透传，一个字节都不动——「网关不改报文」仍然是默认行为。 */
    @Test
    void modelAbsentFromTheTableIsForwardedByteForByte() {
        Outcome outcome = chatAt("/alive" + CHAT_ENTRY, "untouched-xyz");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        String sent = "{\"model\":\"untouched-xyz\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";
        assertThat(upstream.body()).isEqualTo(sent);
        assertThat(Integer.parseInt(upstream.contentLength())).isEqualTo(sent.length());
    }

    /**
     * D9 真正要证的那一条：主厂商连不上、改道到备用厂商之后，请求体不但还在（那个流早在第一次
     * 改写时就被抽干了，靠的是属性里留的原始字节），而且名字是<b>备用厂商</b>的——映射的键是
     * 客户端原始传的 {@code chat-default}，不是已经被换成 {@code dead-real} 的那个。
     */
    @Test
    void failoverReMapsAgainstTheBackupProviderFromTheOriginalName() {
        Outcome outcome = chat("chat-default");

        assertThat(outcome.status())
                .as("主厂商连不上，客户端该拿到备用的 200")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/alive")).isEqualTo(1);
        assertThat(upstream.authorization()).isEqualTo("Bearer alive-secret");
        assertThat(upstream.body())
                .as("改道重投递后请求体必须还在，而且已经换成备用厂商的名字")
                .contains("\"model\":\"alive-real\"");
        assertThat(Integer.parseInt(upstream.contentLength()))
                .isEqualTo(upstream.body().getBytes(StandardCharsets.UTF_8).length);
    }

    /** 手写路由优先于生成的兜底路由（生成的带 order=1000），所以它是真能用的逃生舱。 */
    @Test
    void handwrittenRouteShadowsTheGeneratedFallback() {
        Outcome outcome = chat("shadow-test");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/handwritten")).isEqualTo(1);
        assertThat(upstream.path()).isEqualTo("/upstream/handwritten");
        assertThat(upstream.authorization()).isEqualTo("Bearer handwritten-secret");
    }

    /** 没有 JSON 正文的请求不能在改写这一步被挂住（与 Model 谓词那条同类）。 */
    @Test
    void requestWithoutAJsonBodyDoesNotHangOnTheMappingFilter() {
        Outcome outcome = calls.get("/alive" + CHAT_ENTRY, "Bearer sk-alpha");

        assertThat(outcome.status())
                .as("GET 会命中生成的兜底路由（Model 谓词对非 JSON 直接返回 false），到不了改写那一步")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.requests()).isEqualTo(1);
    }

    /** 兜底厂商认领没人认领的名字，且它自己也在生成表里（这一条同时证明装配真的发生了）。 */
    @Test
    void fallbackProviderPicksUpUnclaimedModels() {
        Outcome outcome = chat("someone-elses-model");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).isEqualTo("/upstream/alive");
        assertThat(upstream.body()).contains("\"model\":\"someone-elses-model\"");
    }

    /** 租户被禁的厂商即使在生成表里也该在碰到厂商之前被挡下（metadata provider 是权限的依据）。 */
    @Test
    void tenantWithoutProviderGrantIsRejectedBeforeReachingTheProvider() {
        Outcome outcome = calls.call(CHAT_ENTRY, "Bearer sk-nobody", "{\"model\":\"chat-default\"}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(upstream.requests()).isZero();
    }
}
