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
        RESET
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

    @Override
    public void close() {
        server.disposeNow(Duration.ofSeconds(5));
    }
}
