package org.hongxi.ai.gateway.support;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

/**
 * 进程内上游桩，把「网关到底做了什么」变成可断言的事实：它记录收到的方法、路径、请求头与请求体，
 * 并按场景决定响应是非流式 JSON、稳定节奏的 SSE、吐一块就沉默、还是整段不回话。
 *
 * <p>不用真实厂商做这些断言：超时与流式语义要毫秒级的节奏控制，真实端点给不了可重复的时序。
 * 真实调用在 {@code ProviderLiveTest}，按每家的环境变量开关。
 */
public final class StubUpstream implements AutoCloseable {

    public enum Scenario {
        /** 立即返回一个非流式 JSON。 */
        ECHO_JSON,
        /** 每 80ms 一块共 5 块，再加结束标记：总时长故意超过路由的 response-timeout。 */
        SSE_STEADY,
        /** 吐一块之后不再说话：模拟厂商开始流式后卡住。 */
        SSE_THEN_SILENCE,
        /** 5 秒后才回第一个字节：模拟厂商接了连接但不出首块。 */
        SILENT,
        /** 直接回 500：模拟厂商服务端故障，是熔断要认的那一类。 */
        FAIL_500,
        /** 吐一块之后把连接弄断：流已经开始下发，此时没法再改道备用厂商。 */
        SSE_THEN_BREAK,
        /** 1.5 秒后才回响应头：比 resilience4j TimeLimiter 的默认 1 秒慢，比模型调用快。 */
        SLOW_HEAD,
        /** 连接接了但立刻报错断开：模拟厂商进程被 kill -9，传输层失败。 */
        RESET,
        /** 非流式 JSON，带完整 usage（含 cached / reasoning 明细）。 */
        ECHO_JSON_WITH_USAGE,
        /** 流式：usage 挂在 finish_reason 那个块上（DeepSeek 的形状）。 */
        SSE_USAGE_ON_FINISH_CHUNK,
        /** 流式：{@code [DONE]} 之前多给一个 {@code choices:[]} 的块带 usage（DashScope 的形状）。 */
        SSE_USAGE_IN_EXTRA_CHUNK,
        /** 200 + OpenAI 兼容错误体（顶层 {@code error}，没有 {@code choices}）：R14 的结构规则。 */
        JSON_200_ERROR_BODY,
        /** 200 + 顶层 {@code code} 自报失败：R14 的 indicator 规则，只在该厂商声明过这个码时才命中。 */
        JSON_200_TOP_LEVEL_CODE,
        /** 429 + OpenAI 兼容限流体：R16 用它验证"被限流的那把 key 单独冷却"。 */
        TOO_MANY_REQUESTS
    }

    /** 稳定节奏场景里每块的间隔。 */
    public static final Duration CHUNK_GAP = Duration.ofMillis(80);

    /** 稳定节奏场景的完整正文，测试用它核对逐字节透传。 */
    public static final String STEADY_BODY = "Hello world";

    private DisposableServer server;
    private final AtomicReference<Scenario> scenario = new AtomicReference<>(Scenario.ECHO_JSON);
    private final AtomicReference<String> method = new AtomicReference<>();
    private final AtomicReference<String> path = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> tenant = new AtomicReference<>();
    private final AtomicReference<String> traceparent = new AtomicReference<>();
    private final AtomicReference<String> body = new AtomicReference<>();
    private final AtomicReference<String> contentLength = new AtomicReference<>();
    private final AtomicInteger requests = new AtomicInteger();
    private final List<String> paths = new java.util.concurrent.CopyOnWriteArrayList<>();

    private StubUpstream(DisposableServer server) {
        this.server = server;
    }

    /** 绑到 127.0.0.1 的随机端口，端口由 {@link #port()} 读回。 */
    public static StubUpstream start() {
        StubUpstream stub = new StubUpstream(null);
        stub.server = HttpServer.create()
                .host("127.0.0.1")
                .port(0)
                .handle(stub::respond)
                .bindNow();
        return stub;
    }

    public int port() {
        return server.port();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + port();
    }

    public void scenario(Scenario next) {
        scenario.set(next);
    }

    public String method() {
        return method.get();
    }

    public String path() {
        return path.get();
    }

    public String authorization() {
        return authorization.get();
    }

    public String tenant() {
        return tenant.get();
    }

    /** W3C 传播头，用来证明链路真的穿过网关接到了厂商。 */
    public String traceparent() {
        return traceparent.get();
    }

    public String body() {
        return body.get();
    }

    /**
     * 桩看到的 {@code Content-Length}。D9 改写请求体之后长度必须重算，这条断言就是钉它的：
     * 长度与字节不一致时，厂商侧要么截断要么拒收，而那在网关日志里长得像厂商的毛病。
     */
    public String contentLength() {
        return contentLength.get();
    }

    public int requests() {
        return requests.get();
    }

    public void reset() {
        requests.set(0);
        paths.clear();
    }

    /** 每个请求落在桩上的路径，按到达顺序。failover 测试靠它区分主备。 */
    public List<String> paths() {
        return List.copyOf(paths);
    }

    public int hits(String path) {
        return (int) paths.stream().filter(path::equals).count();
    }

    private Mono<Void> respond(HttpServerRequest request, HttpServerResponse response) {
        return request.receive().aggregate().asString(StandardCharsets.UTF_8)
                .defaultIfEmpty("")
                .flatMap(payload -> {
                    method.set(request.method().name());
                    path.set(request.fullPath());
                    authorization.set(request.requestHeaders().get(HttpHeaders.AUTHORIZATION));
                    tenant.set(request.requestHeaders().get("X-Tenant-Id"));
                    traceparent.set(request.requestHeaders().get("traceparent"));
                    body.set(payload);
                    contentLength.set(request.requestHeaders().get(HttpHeaders.CONTENT_LENGTH));
                    requests.incrementAndGet();
                    paths.add(request.fullPath());
                    return switch (scenario.get()) {
                        case ECHO_JSON -> json(response);
                        case SSE_STEADY -> steadyStream(response);
                        case SSE_THEN_SILENCE -> firstChunkThenSilence(response);
                        case SILENT -> silent(response);
                        case FAIL_500 -> fail500(response);
                        case SSE_THEN_BREAK -> streamThenBreak(response);
                        case SLOW_HEAD -> slowHead(response);
                        case ECHO_JSON_WITH_USAGE -> jsonWithUsage(response);
                        case SSE_USAGE_ON_FINISH_CHUNK -> usageOnFinishChunk(response);
                        case SSE_USAGE_IN_EXTRA_CHUNK -> usageInExtraChunk(response);
                        case JSON_200_ERROR_BODY -> errorBody(response);
                        case JSON_200_TOP_LEVEL_CODE -> topLevelCode(response);
                        case TOO_MANY_REQUESTS -> tooManyRequests(response);
                        case RESET -> {
                            // 一个字节都不写，直接把 socket 关掉——这才是厂商进程被 kill 的样子。
                            // 之前这里写的是 Mono.error(new IOException(...))，reactor-netty 会替它
                            // 回一个 HTTP 500，于是那条用例测的其实是 500 而不是 reset（日志里
                            // "Error starting response. Replying error status" 就是证据）。
                            response.withConnection(connection -> connection.channel().close());
                            yield Mono.empty();
                        }
                    };
                });
    }

    private Mono<Void> json(HttpServerResponse response) {
        String payload = "{\"id\":\"chatcmpl-stub\",\"object\":\"chat.completion\",\"model\":\"stub-model\","
                + "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}";
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .sendString(Mono.just(payload))
                .then();
    }

    private Mono<Void> steadyStream(HttpServerResponse response) {
        Flux<String> paced = Flux.just(
                        delta("He"), delta("llo"), delta(" "), delta("wor"), delta("ld"),
                        "data: {\"id\":\"1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: [DONE]\n\n")
                .delayElements(CHUNK_GAP);
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .header(HttpHeaders.CACHE_CONTROL, "no-cache")
                .sendString(paced)
                .then();
    }

    private static String delta(String text) {
        return "data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"" + text + "\"}}]}\n\n";
    }

    private Mono<Void> firstChunkThenSilence(HttpServerResponse response) {
        Flux<String> stalled = Flux.just(delta("Hi")).concatWith(Mono.never());
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .sendString(stalled)
                .then();
    }

    private Mono<Void> silent(HttpServerResponse response) {
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .sendString(Mono.delay(Duration.ofSeconds(5)).map(tick -> "data: too late\n\n"))
                .then();
    }

    private Mono<Void> slowHead(HttpServerResponse response) {
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .sendString(Mono.delay(Duration.ofMillis(1500)).map(tick -> "{\"id\":\"chatcmpl-slow\"}"))
                .then();
    }

    private Mono<Void> fail500(HttpServerResponse response) {
        return response.status(500)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .sendString(Mono.just("{\"error\":{\"message\":\"upstream exploded\",\"type\":\"server_error\"}}"))
                .then();
    }

    /**
     * 先正常吐一块（响应头就此提交给客户端），等 300ms 再把流掐断。
     *
     * <p>那 300ms 是必须的：直接 concatWith(Mono.error(...)) 时，错误可能赶在第一块 flush 出去
     * 之前把连接关掉，客户端一个字节都收不到，用例就在「收到一块后断」和「什么都没收到」之间飘。
     */
    private Mono<Void> streamThenBreak(HttpServerResponse response) {
        Flux<String> broken = Flux.just(delta("Hi"))
                .concatWith(Mono.delay(Duration.ofMillis(300))
                        .flatMap(tick -> Mono.error(new IllegalStateException("upstream went away mid-stream"))));
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .sendString(broken)
                .then();
    }

    /**
     * 三种形状共用那一份用量。R12 的验收标准是"三种形状、同一个数"，所以这四个数必须被同一条
     * 扫描规则从流式两种分块位置和非流式整段里都捞出来。
     */
    public static final String USAGE_JSON = "\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,"
            + "\"total_tokens\":18,\"prompt_tokens_details\":{\"cached_tokens\":4},"
            + "\"completion_tokens_details\":{\"reasoning_tokens\":3}}";

    /** 厂商自己的请求 id，R13 的 upstreamRequestId 断的就是它。 */
    public static final String REQUEST_ID = "stub-request-1";

    private Mono<Void> jsonWithUsage(HttpServerResponse response) {
        String payload = "{\"id\":\"chatcmpl-usage\",\"object\":\"chat.completion\",\"model\":\"stub-model\","
                + "\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}],"
                + USAGE_JSON + "}";
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("x-request-id", REQUEST_ID)
                .sendString(Mono.just(payload))
                .then();
    }

    private Mono<Void> usageOnFinishChunk(HttpServerResponse response) {
        Flux<String> stream = Flux.just(
                delta("He"),
                "data: {\"id\":\"1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],"
                        + USAGE_JSON + "}\n\n",
                "data: [DONE]\n\n");
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .header("x-request-id", REQUEST_ID)
                .sendString(stream)
                .then();
    }

    /**
     * DashScope 的形状：{@code finish_reason} 那个块不带 usage，{@code [DONE]} 之前再多给一个
     * {@code choices:[]} 的块。"取倒数第二个块"就是在这里错的——两种形状在同一个位置捞，
     * 得到的一个是数、一个是什么都没有。
     */
    private Mono<Void> usageInExtraChunk(HttpServerResponse response) {
        Flux<String> stream = Flux.just(
                delta("He"),
                "data: {\"id\":\"1\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: {\"id\":\"1\",\"choices\":[]," + USAGE_JSON + "}\n\n",
                "data: [DONE]\n\n");
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE)
                .header("x-request-id", REQUEST_ID)
                .sendString(stream)
                .then();
    }

    /** OpenAI 兼容的错误形状：顶层 error、没有 choices。 */
    public static final String ERROR_BODY = "{\"error\":{\"message\":\"Too many requests\","
            + "\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\"}}";

    /** 厂商在顶层 code 上自报失败：没有 error 对象，结构规则不会命中。 */
    public static final String TOP_LEVEL_CODE_BODY = "{\"code\":\"Throttling.AllocationQuota\","
            + "\"message\":\"Queue capacity limit exceeded, please retry later\"}";

    /** 厂商按 key 给配额，所以 429 是"这把 key 该让路"最明确的信号。 */
    public static final String RATE_LIMITED_BODY = "{\"error\":{\"message\":\"Rate limit reached for this key\","
            + "\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\"}}";

    private Mono<Void> tooManyRequests(HttpServerResponse response) {
        return response.status(429)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header(HttpHeaders.RETRY_AFTER, "30")
                .sendString(Mono.just(RATE_LIMITED_BODY))
                .then();
    }

    private Mono<Void> errorBody(HttpServerResponse response) {
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("x-request-id", REQUEST_ID)
                .sendString(Mono.just(ERROR_BODY))
                .then();
    }

    private Mono<Void> topLevelCode(HttpServerResponse response) {
        return response.status(200)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header("x-request-id", REQUEST_ID)
                .sendString(Mono.just(TOP_LEVEL_CODE_BODY))
                .then();
    }

    @Override
    public void close() {
        server.disposeNow(Duration.ofSeconds(5));
    }
}
