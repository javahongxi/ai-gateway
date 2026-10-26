package org.hongxi.ai.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R3 模型路由与灰度：同一个 OpenAI 兼容入口，按请求体里的 {@code model} 选厂商；同一模型还能按
 * 权重把流量切给两条路由。
 *
 * <p>顺带把文档里最后一条未测项结掉：{@code Weight} 与「要读请求体的谓词」挂在同一条路由上会怎样
 * （见 {@link #weightSplitsTrafficBetweenTwoRoutes}）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        // 关掉厂商窗口这一维（属性名写错不会报错，只会静默用默认值 20，40 次必然撞 429）
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=qwen",
        "ai-gateway.tenants.alpha.providers[1]=deepseek",
        "ai-gateway.tenants.alpha.providers[2]=grey-a",
        "ai-gateway.tenants.alpha.providers[3]=grey-b"
})
@Import(AlwaysAllowRateLimiter.class)
class ModelRoutingTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String WEIGHTED_PATH = "/weighted/v1/chat/completions";

    private static final String BODY_TEMPLATE = "{\"model\":\"%s\",\"messages\":[]}";

    private static final String BODY = "{\"model\":\"qwen3.7-plus\",\"messages\":[]}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        // deepseek* 与 qwen* 两条路由共用同一个入口路径，靠 Model 谓词分开
        modelRoute(registry, 0, "to-deepseek", "deepseek", "deepseek*");
        modelRoute(registry, 1, "to-qwen", "qwen", "qwen*");
        // 灰度：同一个模型两组权重，5:5
        weightedRoute(registry, 2, "grey-a", "grey-a", 50);
        weightedRoute(registry, 3, "grey-b", "grey-b", 50);
    }

    private static void modelRoute(DynamicPropertyRegistry registry, int index, String id, String provider,
            String pattern) {
        String route = "spring.cloud.gateway.server.webflux.routes[" + index + "]";
        registry.add(route + ".id", () -> id);
        registry.add(route + ".uri", upstream::baseUrl);
        registry.add(route + ".predicates[0]", () -> "Path=" + CHAT_PATH);
        registry.add(route + ".predicates[1].name", () -> "Model");
        registry.add(route + ".predicates[1].args[patterns]", () -> pattern);
        registry.add(route + ".metadata.provider", () -> provider);
        registry.add(route + ".filters[0]", () -> "SetPath=/upstream/" + provider);
    }

    private static void weightedRoute(DynamicPropertyRegistry registry, int index, String id, String provider,
            int weight) {
        String route = "spring.cloud.gateway.server.webflux.routes[" + index + "]";
        registry.add(route + ".id", () -> id);
        registry.add(route + ".uri", upstream::baseUrl);
        registry.add(route + ".predicates[0]", () -> "Path=" + WEIGHTED_PATH);
        registry.add(route + ".predicates[1].name", () -> "Model");
        registry.add(route + ".predicates[1].args[patterns]", () -> "grey*");
        registry.add(route + ".predicates[2]", () -> "Weight=grey-group, " + weight);
        registry.add(route + ".metadata.provider", () -> provider);
        registry.add(route + ".filters[0]", () -> "SetPath=/upstream/" + provider);
    }

    @BeforeEach
    void connect() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, BODY);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    /** 请求体里只有 model 这一处不同，路由就得按它分流。 */
    private Outcome callWithModel(String path, String model) {
        return calls.call(path, "Bearer sk-alpha", String.format(BODY_TEMPLATE, model));
    }

    @Test
    void modelDecidesTheProviderBehindOneSharedPath() {
        assertThat(callWithModel(CHAT_PATH, "deepseek-v4-flash").status())
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).isEqualTo("/upstream/deepseek");

        upstream.reset();
        assertThat(callWithModel(CHAT_PATH, "qwen3.7-plus").status())
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).isEqualTo("/upstream/qwen");
    }

    @Test
    void theBodyStillReachesTheProviderByteForByteAfterThePredicateReadsIt() {
        String body = callWithModel(CHAT_PATH, "qwen3.7-plus").body();

        assertThat(upstream.body())
                .as("谓词读 body 用的是 cacheRequestBodyAndRequest，转发时必须能重读")
                .isEqualTo("{\"model\":\"qwen3.7-plus\",\"messages\":[]}");
        assertThat(body).contains("chatcmpl-stub");
    }

    @Test
    void aModelNobodyClaimsIsNotFoundRatherThanSentSomewhere() {
        assertThat(callWithModel(CHAT_PATH, "glm-4-plus").status())
                .as("两条 Model 谓词都不匹配 → 没有路由命中")
                .isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void weightAndABodyReadingPredicateCoexistOnTheSameGroup() {
        int a = 0;
        int b = 0;
        for (int i = 0; i < 40; i++) {
            assertThat(callWithModel(WEIGHTED_PATH, "grey-model").status())
                    .as("第 %d 次请求该被某一条灰度路由接住", i)
                    .isEqualTo(HttpStatus.OK.value());
            if ("/upstream/grey-a".equals(upstream.path())) {
                a++;
            }
            else {
                b++;
            }
        }

        assertThat(a).as("5:5 的权重，两边都该分到流量").isGreaterThan(5);
        assertThat(b).as("5:5 的权重，两边都该分到流量").isGreaterThan(5);
        assertThat(a + b).isEqualTo(40);
        // 结掉文档里最后一条未测项：Weight 先算（它是普通谓词），Model 谓词读到的 body 在两条
        // 候选路由之间是共享的缓存，第二次命中不再重新读 body——所以没有出现「body 已被消费」的错。
    }

    @Test
    void requestWithoutAJsonBodyDoesNotHangOnThePredicate() {
        // GET /v1/models 这类没有正文的请求：谓词直接判否，不该把连接挂住
        assertThat(calls.get(CHAT_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(upstream.requests()).isZero();
    }
}
