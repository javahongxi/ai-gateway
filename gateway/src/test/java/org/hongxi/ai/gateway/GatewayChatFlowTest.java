package org.hongxi.ai.gateway;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.reactive.function.client.WebClient;

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
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub",
        "ai-gateway.tenants.beta.api-key=sk-beta",
        "ai-gateway.tenants.beta.providers[0]=someone-else",
        "ai-gateway.tenants.gamma.api-key=sk-gamma",
        "ai-gateway.tenants.gamma.providers[0]=stub2"
})
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

    private WebClient client;

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
        // 直接 WebClient.create：Boot 4 把 WebClient 的自动配置拆进了独立模块
        // spring-boot-webclient（org.springframework.boot.webclient.WebClientCustomizer 在那儿），
        // 网关的依赖树里没有它，WebClient.Builder 这个 bean 不存在；WebClient 类本身来自
        // spring-webflux，测试不必为了一个 Builder 加依赖
        client = WebClient.create("http://127.0.0.1:" + port);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    @Test
    void missingApiKeyIsRejectedBeforeReachingTheProvider() {
        Outcome outcome = call(null);

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(outcome.body()).contains("\"code\":\"invalid_api_key\"");
        assertThat(upstream.requests()).as("没有身份的请求不该碰到厂商").isZero();
    }

    @Test
    void unknownApiKeyIsRejectedWithTheOpenAiErrorEnvelope() {
        Outcome outcome = call("Bearer sk-not-a-real-key");

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
        Outcome outcome = call("Bearer sk-beta");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("provider_not_allowed");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void eachProviderRouteCarriesItsOwnPathAndCredential() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);

        Outcome outcome = call(SECOND_CHAT_PATH, "Bearer sk-gamma");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.path()).as("第二条路由改写自己的厂商路径").isEqualTo("/upstream/second");
        assertThat(upstream.authorization()).isEqualTo("Bearer second-secret");
        assertThat(upstream.tenant()).isEqualTo("gamma");
    }

    @Test
    void tenantAllowedForOneProviderCannotUseAnotherProvidersRoute() {
        Outcome outcome = call(SECOND_CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.FORBIDDEN.value());
        assertThat(outcome.body()).contains("stub2");
        assertThat(upstream.requests()).isZero();
    }

    @Test
    void nonStreamCallIsForwardedWithTheProviderCredentialAndAnUnchangedBody() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);

        Outcome outcome = call("Bearer sk-alpha");

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

        Outcome outcome = call("Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(reassembleDeltas(outcome.body())).isEqualTo(StubUpstream.STEADY_BODY);
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

        Outcome outcome = call("Bearer sk-alpha");

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
        // 桩吐一块之后不再说话：这是 R11 的缺口，断言钉住当前行为

        LiveCall live = new LiveCall("Bearer sk-alpha");
        live.awaitQuietly(Duration.ofMillis(1500));

        assertThat(live.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(live.body()).as("第一块已经交付给客户端").contains("Hi");
        assertThat(live.terminated()).as("1.5 秒后流仍未结束：正文阶段没有空闲超时").isFalse();
        live.cancel();
    }

    private Outcome call(String authorizationHeader) {
        return call(CHAT_PATH, authorizationHeader);
    }

    private Outcome call(String path, String authorizationHeader) {
        long started = System.nanoTime();
        AtomicInteger status = new AtomicInteger();
        List<String> pieces = new CopyOnWriteArrayList<>();
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            client.post()
                    .uri(path)
                    .contentType(MediaType.APPLICATION_JSON)
                    .headers(headers -> setAuth(headers, authorizationHeader))
                    .bodyValue(REQUEST_BODY)
                    .exchangeToFlux(response -> {
                        status.set(response.statusCode().value());
                        return response.bodyToFlux(DataBuffer.class);
                    })
                    .doOnNext(buffer -> {
                        pieces.add(read(buffer));
                        arrivals.add(elapsed(started));
                    })
                    .then()
                    .block(Duration.ofSeconds(10));
        }
        catch (RuntimeException timeoutOrAbort) {
            failure.set(timeoutOrAbort);
        }
        return new Outcome(status.get(), String.join("", pieces), elapsed(started), List.copyOf(arrivals),
                failure.get());
    }

    /** {@code null} 表示不带 Authorization，用来测「无 key」。 */
    private static void setAuth(HttpHeaders headers, String authorizationHeader) {
        if (authorizationHeader != null) {
            headers.set(HttpHeaders.AUTHORIZATION, authorizationHeader);
        }
    }

    private static long elapsed(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    private static String read(DataBuffer buffer) {
        String text = buffer.toString(StandardCharsets.UTF_8);
        DataBufferUtils.release(buffer);
        return text;
    }

    /**
     * 把 SSE 里的 {@code delta.content} 拼回原文。手写解析够用且不引依赖：桩的输出形状固定。
     */
    private static String reassembleDeltas(String sse) {
        StringBuilder text = new StringBuilder();
        for (String line : sse.split("\n")) {
            int marker = line.indexOf("\"content\":\"");
            if (marker < 0) {
                continue;
            }
            int from = marker + "\"content\":\"".length();
            int to = line.indexOf('"', from);
            if (to > from) {
                text.append(line, from, to);
            }
        }
        return text.toString();
    }

    /**
     * 一次已经发出的请求，能在它还在跑的时候观察状态码、已到达的分块与到达时刻。
     */
    private final class LiveCall {

        private final AtomicInteger status = new AtomicInteger();
        private final List<String> pieces = new CopyOnWriteArrayList<>();
        private final List<Long> arrivals = new CopyOnWriteArrayList<>();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private final Disposable subscription;

        private LiveCall(String authorizationHeader) {
            Flux<DataBuffer> response = client.post()
                    .uri(CHAT_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .headers(headers -> setAuth(headers, authorizationHeader))
                    .bodyValue(REQUEST_BODY)
                    .exchangeToFlux(it -> {
                        status.set(it.statusCode().value());
                        return it.bodyToFlux(DataBuffer.class);
                    });
            long started = System.nanoTime();
            this.subscription = response
                    .doOnNext(buffer -> {
                        pieces.add(read(buffer));
                        arrivals.add((System.nanoTime() - started) / 1_000_000);
                    })
                    .then()
                    .subscribe(ignored -> {
                    }, error -> terminated.set(true), () -> terminated.set(true));
        }

        void awaitQuietly(Duration patience) {
            long deadline = System.nanoTime() + patience.toNanos();
            while (System.nanoTime() < deadline && !terminated.get()) {
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        int status() {
            return status.get();
        }

        String body() {
            return String.join("", pieces);
        }

        boolean terminated() {
            return terminated.get();
        }

        void cancel() {
            subscription.dispose();
        }
    }

    /** 一次请求的完整观测结果。 */
    private record Outcome(int status, String body, long elapsed, List<Long> arrivals, Throwable failure) {
    }
}
