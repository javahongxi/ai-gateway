package org.hongxi.ai.gateway.observability;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;
import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.routing.UpstreamKeyPool;
import org.hongxi.ai.gateway.routing.ModelMappingGatewayFilterFactory;
import org.hongxi.ai.gateway.routing.ModelRoutePredicateFactory;

/**
 * 响应侧唯一的观察者：把交给客户端的字节<b>只读一遍</b>，然后分三个出口记账——token 用量（R12）、
 * 语义失败（R14）、访问日志一行（R13）。
 *
 * <p><b>为什么是一个观察者三个出口。</b>这三件事读的是同一份正文。分开放有过两个失败版本：第一版让
 * 日志从 exchange 属性里读内层采到的结果，实测读不到——内层在 {@code doFinally} 里写的属性，外层的
 * {@code doFinally} 看不见（现象是"单跑绿、整包红"，而日志里 status 明明写着 200；同一张表上请求阶段
 * 写的租户与模型名却一直读得到）。现象被钉在 {@code AttributeVisibilityTest}，成因未验，所以这里只按
 * 现象写规矩。第二版改成各看各的字节，于是要读两遍；R14 是第三个消费者，三遍说不下去。所以现在是
 * 一份判断逻辑（本类的静态方法）+ 一次读字节，代价与耦合都最小。见 docs 决策 D11。
 *
 * <p><b>只读，一个字节都不改</b>（决策 D1）：{@link DataBuffer#toString(java.nio.charset.Charset)}
 * 按 position 取数，不推动 readerIndex，peek 完下游拿到的还是完整的一份。累积有上限
 * （{@value #CAPTURE_LIMIT_CHARS} 字符），超限就停——计量与排障都不该把一个几十 MB 的响应留在堆里。
 *
 * <p><b>状态码在写字节那一刻记</b>，不在 {@code doFinally} 里读：那时可能还是 null，一律当非 2xx
 * 就会把所有正常响应判成"没发生"，用量全线静默归零（实测踩过）。
 */
@Component
public class ResponseObservationFilter implements GlobalFilter, Ordered {

    /** token 计数，按 {@code type} 分四条：prompt / completion / cached / reasoning。 */
    public static final String USAGE_TOKENS_METRIC = "ai.gateway.usage.tokens";

    /** 厂商有没有回 usage（{@code outcome=present|absent}）。没回时记 absent，不记 0。 */
    public static final String USAGE_SAMPLES_METRIC = "ai.gateway.usage.samples";

    /** 200 但正文里是失败（{@code kind=structural|indicator}），R14。 */
    public static final String SEMANTIC_FAILURE_METRIC = "ai.gateway.semantic.failures";

    /** 访问日志的 logger 名，运维要单独收集或关掉就靠它。 */
    public static final String ACCESS_LOGGER = "ai.gateway.access";

    static final int CAPTURE_LIMIT_CHARS = 1 << 18;

    /** 厂商回自己请求 id 的头名，各家不统一，按顺序找第一个有的。 */
    private static final List<String> UPSTREAM_REQUEST_ID_HEADERS =
            List.of("x-request-id", "request-id", "x-requestid", "x-amzn-requestid", "x-dashscope-request-id");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger access = LoggerFactory.getLogger(ACCESS_LOGGER);

    /** 每家厂商当前的连续语义命中数，正常响应即清零（判连续不判累计）。 */
    private final Map<String, Integer> consecutiveHits = new java.util.concurrent.ConcurrentHashMap<>();

    private final MeterRegistry registry;

    private final CircuitBreakerRegistry circuitBreakers;

    private final AiGatewayProperties properties;

    private final UpstreamKeyPool keyPool;

    public ResponseObservationFilter(MeterRegistry registry, CircuitBreakerRegistry circuitBreakers,
            AiGatewayProperties properties, UpstreamKeyPool keyPool) {
        this.registry = registry;
        this.circuitBreakers = circuitBreakers;
        this.properties = properties;
        this.keyPool = keyPool;
    }

    /** 一次调用采到的四个数。整体缺席时压根不产出这个对象，而不是全 0。 */
    public record Usage(long promptTokens, long completionTokens, long cachedTokens, long reasoningTokens) {
    }

    /**
     * 一次语义命中。{@code kind} 是 {@code structural}（OpenAI 兼容错误体）或
     * {@code indicator}（厂商在顶层 code 上自报的失败码），{@code detail} 是命中了什么。
     */
    public record SemanticFailure(String kind, String detail) {
    }

    @Override
    public int getOrder() {
        // 比所有路由内过滤器都早，所以鉴权短路、限流拒绝、没有路由匹配的 404 也都会留下一行
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long started = System.nanoTime();
        StringBuilder captured = new StringBuilder();
        boolean[] capped = {false};
        int[] statusAtFirstWrite = {-1};
        AtomicLong firstByteAt = new AtomicLong(-1L);

        ServerHttpResponse decorated = new ServerHttpResponseDecorator(exchange.getResponse()) {

            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                note(statusAtFirstWrite, getStatusCode());
                firstByteAt.compareAndSet(-1L, System.nanoTime());
                return super.writeWith(tee(Flux.from(body), captured, capped));
            }

            @Override
            public Mono<Void> writeAndFlushWith(
                    org.reactivestreams.Publisher<? extends org.reactivestreams.Publisher<? extends DataBuffer>> body) {
                note(statusAtFirstWrite, getStatusCode());
                firstByteAt.compareAndSet(-1L, System.nanoTime());
                Flux<Flux<DataBuffer>> nested = Flux.from(body)
                        .map(inner -> tee(Flux.from(inner), captured, capped));
                return super.writeAndFlushWith(nested);
            }
        };

        return chain.filter(exchange.mutate().response(decorated).build())
                .doFinally(signal -> settle(exchange, started, signal.name(), captured.toString(),
                        statusAtFirstWrite[0], firstByteAt.get()));
    }

    private static void note(int[] slot, HttpStatusCode status) {
        if (slot[0] < 0 && status != null) {
            slot[0] = status.value();
        }
    }

    static Flux<DataBuffer> tee(Flux<DataBuffer> body, StringBuilder sink, boolean[] capped) {
        return body.doOnNext(buffer -> {
            if (capped[0]) {
                return;
            }
            sink.append(buffer.toString(java.nio.charset.StandardCharsets.UTF_8));
            if (sink.length() > CAPTURE_LIMIT_CHARS) {
                capped[0] = true;
            }
        });
    }

    private void settle(ServerWebExchange exchange, long started, String signal, String captured,
            int status, long firstByteNanos) {
        String tenant = tenantOf(exchange);
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        String provider = route == null ? "none" : String.valueOf(route.getMetadata().get("provider"));
        boolean successfulHttp = status >= 200 && status < 300;

        // 被拒（401/403/429）与首字节前失败（504）的不产生用量与语义判定：那次调用没在厂商发生
        Usage usage = successfulHttp ? extract(captured) : null;
        SemanticFailure semantic = successfulHttp ? detectSemanticFailure(captured, indicatorsOf(provider)) : null;
        if (successfulHttp) {
            account(captured, usage, semantic, tenant, provider);
        }
        // 上游回 429，或者 200 里自报限流：把这次用的那把 key 关进冷却（R16）。按 key 而不是按厂商，
        // 是因为多把 key 时只有被限流的那把该让路——这正是密钥池存在的理由。
        if (status == 429 || semantic != null) {
            Object rawKey = exchange.getAttributes().get(UpstreamKeyPool.SELECTED_KEY_ATTRIBUTE);
            if (rawKey instanceof String key) {
                keyPool.coolDown(provider, key);
            }
        }

        writeLine(exchange, started, signal, tenant, route, provider, status, firstByteNanos, usage, semantic);
    }

    private void account(String captured, Usage usage, SemanticFailure semantic, String tenant, String provider) {
        if (usage != null) {
            sample("present", tenant, provider).increment();
            token("prompt", tenant, provider).increment(usage.promptTokens());
            token("completion", tenant, provider).increment(usage.completionTokens());
            token("cached", tenant, provider).increment(usage.cachedTokens());
            token("reasoning", tenant, provider).increment(usage.reasoningTokens());
        }
        else {
            sample("absent", tenant, provider).increment();
        }
        if (semantic == null) {
            // 厂商这次答得正常：连续计数清零。判"连续"不判"累计"，就是为了让偶发一次限流
            // 不至于把好好的一家厂商标成故障
            consecutiveHits.remove(provider);
            return;
        }
        Counter.builder(SEMANTIC_FAILURE_METRIC)
                .description("HTTP 200 但正文里是失败")
                .tag("kind", semantic.kind())
                .tag("provider", provider)
                .register(registry)
                .increment();

        int hits = consecutiveHits.merge(provider, 1, Integer::sum);
        int threshold = Math.max(1, properties.getSemanticFailureThreshold());
        if (hits < threshold) {
            return;
        }
        // 到阈值就直接开闸，而不是往熔断的滑动窗口里补记一次 onError：那台状态机已经把这发请求
        // 记成了一次 success，再塞一个事件等于同一发占两个位置、失败率立刻 50%，一发就能开闸。
        // 当前这一发的响应已经发出去了，撤不回来（D6.3），被绕开的是后面的请求。
        consecutiveHits.put(provider, 0);
        circuitBreakers.find(provider).ifPresent(breaker -> {
            if (breaker.getState() != CircuitBreaker.State.OPEN) {
                breaker.transitionToOpenState();
            }
        });
    }

    private List<String> indicatorsOf(String provider) {
        AiGatewayProperties.Provider declared = properties.getProviders().get(provider);
        if (declared == null || declared.getFailureIndicators() == null) {
            return List.of();
        }
        return declared.getFailureIndicators();
    }

    private Counter sample(String outcome, String tenant, String provider) {
        return Counter.builder(USAGE_SAMPLES_METRIC)
                .description("厂商有没有回 usage")
                .tag("outcome", outcome)
                .tag("tenant", tenant)
                .tag("provider", provider)
                .register(registry);
    }

    private Counter token(String type, String tenant, String provider) {
        return Counter.builder(USAGE_TOKENS_METRIC)
                .description("token 用量，按类型分条")
                .tag("type", type)
                .tag("tenant", tenant)
                .tag("provider", provider)
                .register(registry);
    }

    private void writeLine(ServerWebExchange exchange, long started, String signal, String tenant, Route route,
            String provider, int status, long firstByteNanos, Usage usage, SemanticFailure semantic) {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", java.time.Instant.now().toString());
        line.put("tenant", tenant);
        line.put("route", route == null ? "none" : route.getId());
        line.put("provider", provider);
        line.put("outcome", signal);

        // upstream 只有走过改写才有值；requested 在没挂改写的路由上退回到 Model 谓词解析出的名字
        String upstream = ModelMappingGatewayFilterFactory.upstreamModel(exchange);
        String requested = ModelMappingGatewayFilterFactory.requestedModel(exchange);
        if (requested == null) {
            requested = ModelRoutePredicateFactory.parsedModel(exchange);
        }
        line.put("requestedModel", requested);
        line.put("upstreamModel", upstream == null ? requested : upstream);
        line.put("status", status);
        line.put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        if (firstByteNanos >= 0) {
            line.put("ttftMs", TimeUnit.NANOSECONDS.toMillis(firstByteNanos - started));
        }
        line.put("usage", usage == null ? "unknown" : usageMap(usage));
        if (semantic != null) {
            line.put("semanticFailure", semantic);
        }
        // R16：这次用的是哪把 key（只有指纹，原文绝不进日志）。多把 key 时它是排障的第一坐标——
        // "哪家限流了"不如"哪把 key 限流了"有用
        Object keyFingerprint = exchange.getAttributes().get(UpstreamKeyPool.SELECTED_FINGERPRINT_ATTRIBUTE);
        if (keyFingerprint != null) {
            line.put("upstreamKey", keyFingerprint);
        }
        String traceparent = exchange.getRequest().getHeaders().getFirst("traceparent");
        if (traceparent != null) {
            line.put("traceparent", traceparent);
        }
        String upstreamId = upstreamRequestId(exchange);
        if (upstreamId != null) {
            line.put("upstreamRequestId", upstreamId);
        }

        try {
            // 单行：不加缩进，采集端按行切不会把它拆成两条
            access.info(MAPPER.writeValueAsString(line));
        }
        catch (Exception serializationFailed) {
            access.info("ai.gateway.access line unavailable: {}", serializationFailed.getClass().getName());
        }
    }

    private static Map<String, Object> usageMap(Usage usage) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("promptTokens", usage.promptTokens());
        out.put("completionTokens", usage.completionTokens());
        out.put("cachedTokens", usage.cachedTokens());
        out.put("reasoningTokens", usage.reasoningTokens());
        return out;
    }

    private static String tenantOf(ServerWebExchange exchange) {
        Object tenant = exchange.getAttributes().get(TenantAuthFilter.TENANT_ATTRIBUTE);
        return tenant == null ? "anonymous" : tenant.toString();
    }

    private static String upstreamRequestId(ServerWebExchange exchange) {
        HttpHeaders headers = exchange.getResponse().getHeaders();
        for (String name : UPSTREAM_REQUEST_ID_HEADERS) {
            String value = headers.getFirst(name);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    /**
     * 逐条记录扫，取第一条带 {@code completion_tokens} 的。流式（每行一个 {@code data:} 帧）与
     * 非流式（整个正文一条 JSON）走同一段代码。
     *
     * <p>不能假设"倒数第二个块"：同一份请求，DashScope 在 {@code [DONE]} 之前多给一个
     * {@code choices:[]} 的块带 usage，DeepSeek 把 usage 挂在 {@code finish_reason} 那个块上。
     * 单条解析失败就跳过（帧可能被拆坏、可能是注释行）——采不到是 absent，不是异常。
     */
    public static Usage extract(String body) {
        for (String record : records(body)) {
            if (!record.contains("completion_tokens")) {
                continue;
            }
            try {
                JsonNode usage = MAPPER.readTree(record).path("usage");
                if (usage.isObject() && usage.path("completion_tokens").isNumber()) {
                    return new Usage(usage.path("prompt_tokens").asLong(0L),
                            usage.path("completion_tokens").asLong(0L),
                            usage.path("prompt_tokens_details").path("cached_tokens").asLong(0L),
                            usage.path("completion_tokens_details").path("reasoning_tokens").asLong(0L));
                }
            }
            catch (Exception unparsable) {
                // 继续扫下一条，不因为一帧的形状放弃整次计量
            }
        }
        return null;
    }

    /**
     * R14：HTTP 200、正文里却是失败。两条规则，都只看<b>顶层字段</b>：
     *
     * <ul>
     *   <li>{@code structural}：OpenAI 兼容的错误体——顶层有 {@code error} 对象且没有
     *       {@code choices}。这条与厂商无关，是协议形状本身。</li>
     *   <li>{@code indicator}：厂商在顶层 {@code code}（或 {@code error.code} /
     *       {@code error.type}）上自报的失败码，命中该厂商声明的
     *       {@code failure-indicators} 列表。</li>
     * </ul>
     *
     * <p><b>为什么不在正文里做子串搜索</b>：模型的回答里完全可能出现 "quota exceeded"、
     * "rate limit" 这些字——那是要送给用户的文本，不是失败信号。判定只认厂商自己放在结构化字段上的
     * 错误码，宁可漏判也不误判（误判会把一次正常调用记成厂商故障，进而无谓地打开熔断）。
     *
     * <p>哪些厂商有哪些失败码属于部署配置，本文档不声称实测过某家的具体字符串。
     */
    public static SemanticFailure detectSemanticFailure(String body, List<String> indicators) {
        for (String record : records(body)) {
            JsonNode node;
            try {
                node = MAPPER.readTree(record);
            }
            catch (Exception unparsable) {
                continue;
            }
            if (node == null || !node.isObject()) {
                continue;
            }
            JsonNode error = node.path("error");
            if (error.isObject() && !node.has("choices")) {
                return new SemanticFailure("structural", describe(error));
            }
            String code = firstText(node.path("code"), error.path("code"), error.path("type"));
            if (code != null && indicators != null) {
                for (String indicator : indicators) {
                    if (indicator.equalsIgnoreCase(code)) {
                        return new SemanticFailure("indicator", code);
                    }
                }
            }
        }
        return null;
    }

    private static String describe(JsonNode error) {
        String code = firstText(error.path("code"), error.path("type"));
        return code == null ? "error" : code;
    }

    private static String firstText(JsonNode... nodes) {
        for (JsonNode node : nodes) {
            if (node != null && node.isTextual() && !node.asText().isBlank()) {
                return node.asText();
            }
        }
        return null;
    }

    /** 流式按行拆帧并去掉 {@code data:} 前缀；非流式整段就是一条。 */
    private static List<String> records(String body) {
        if (body == null || body.isEmpty()) {
            return List.of();
        }
        List<String> out = new java.util.ArrayList<>();
        for (String line : body.split("\n")) {
            String record = line.strip();
            if (record.isEmpty() || record.startsWith(":")) {
                continue;
            }
            if (record.startsWith("data:")) {
                record = record.substring("data:".length()).strip();
            }
            if (record.isEmpty() || "[DONE]".equals(record)) {
                continue;
            }
            out.add(record);
        }
        return out;
    }
}
