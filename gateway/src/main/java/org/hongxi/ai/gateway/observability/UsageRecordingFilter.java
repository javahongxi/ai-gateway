package org.hongxi.ai.gateway.observability;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.auth.TenantAuthFilter;

/**
 * R12：把一次模型调用用掉多少 token 采下来，落成指标。
 *
 * <p>访问日志（R13）需要同一份数，但它自己再看一遍响应字节、复用本类的 {@link #extract}
 * 规则，而不是从本类接一个属性——exchange 被 mutate 过之后，内层写的属性在外层的
 * {@code doFinally} 里读不到（实测见 docs 决策 D11）。宁可多读一遍几 KB，也不留一条会悄悄失效的
 * 跨层通道。
 *
 * <p><b>采集不等于结算。</b>本类只回答"这次用了多少"，不回答"该收谁的钱"——倍率、余额、出账在
 * 边界之外（见文档第一节）。仍然要做，是因为厂商返回的 usage 是唯一权威的用量数，而它同时是三处的
 * 输入：按 token 扣限的 R8（{@code tenant-requested-tokens} 那句注释早就想要它）、成本指标，以及
 * 排障时必须能看到的"这次到底吐了多少"。
 *
 * <p><b>扫描规则是被实测逼出来的。</b>不能假设"倒数第二个块"：同一份请求，DashScope 在
 * {@code [DONE]} 之前<b>多给一个</b> {@code choices:[]} 的块带 {@code usage}，DeepSeek 把
 * {@code usage} 挂在 {@code finish_reason=stop} 那个块上。所以做法是<b>逐块扫，取第一个带
 * {@code completion_tokens} 的</b>——两种形状因此得到同一个数（两条用例分别钉住）。非流式是这条
 * 规则的退化情况：整段正文就一条记录。
 *
 * <p><b>厂商没回 usage 时记 absent，不记 0。</b>把"没采到"和"用了 0 个 token"混成同一个数是指标
 * 最典型的说谎方式，而实测确实会这样：同一份请求直连三次都带 usage，走网关的一次没带。
 *
 * <p><b>只读，不改一个字节</b>（与决策 D1 一致）。挂点跟首字延迟同一个位置，只是再早一层：两者都是
 * 响应装饰器，谁在外面看到的字节都一样。peek 用 {@link DataBuffer#toString(java.nio.charset.Charset)}
 * ——它按 position 取数、不推动 readerIndex，所以 peek 完下游拿到的还是完整的体。扫描有上限
 * （{@value #CAPTURE_LIMIT_CHARS} 字符），超限就停：计量不该把一个几十 MB 的响应留在堆里。
 */
@Component
public class UsageRecordingFilter implements GlobalFilter, Ordered {


    /** token 计数，按 {@code type} 分四条：prompt / completion / cached / reasoning。 */
    public static final String USAGE_TOKENS_METRIC = "ai.gateway.usage.tokens";

    /** 采样有无（{@code outcome=present|absent}），暴露"厂商没回 usage"这件事本身。 */
    public static final String USAGE_SAMPLES_METRIC = "ai.gateway.usage.samples";

    static final int CAPTURE_LIMIT_CHARS = 1 << 18;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MeterRegistry registry;

    public UsageRecordingFilter(MeterRegistry registry) {
        this.registry = registry;
    }

    /** 一次调用采到的四个数。整体缺席时压根不产出这个对象，而不是全 0。 */
    public record Usage(long promptTokens, long completionTokens, long cachedTokens, long reasoningTokens) {
    }

    @Override
    public int getOrder() {
        // 早于 NettyWriteResponseFilter(-1) 与首字延迟(-2)，才能看到交给客户端的字节
        return -3;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        StringBuilder captured = new StringBuilder();
        boolean[] capped = {false};
        // 状态要在**写字节的那一刻**记：doFinally 的完成信号从内层往外冒，本过滤器比外层过滤器先
        // 结束，那时读 response.getStatusCode() 可能还是 null（实测：日志行里 status 明明是 200，
        // 这里读到 null，于是所有正常响应都被判成"没发生"）
        int[] statusAtFirstWrite = {-1};

        ServerHttpResponse decorated = new ServerHttpResponseDecorator(exchange.getResponse()) {

            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                noteStatus(statusAtFirstWrite, getStatusCode());
                return super.writeWith(peekBytes(Flux.from(body), captured, capped));
            }

            @Override
            public Mono<Void> writeAndFlushWith(
                    org.reactivestreams.Publisher<? extends org.reactivestreams.Publisher<? extends DataBuffer>> body) {
                noteStatus(statusAtFirstWrite, getStatusCode());
                Flux<Flux<DataBuffer>> nested = Flux.from(body)
                        .map(inner -> peekBytes(Flux.from(inner), captured, capped));
                return super.writeAndFlushWith(nested);
            }
        };

        return chain.filter(exchange.mutate().response(decorated).build())
                .doFinally(signal -> record(exchange, captured.toString(), statusAtFirstWrite[0]));
    }

    /** 供本类与访问日志（R13）共用：只 peek 字节、不消费，超过上限就停止累积。 */
    static Flux<DataBuffer> peekBytes(Flux<DataBuffer> body, StringBuilder captured, boolean[] capped) {
        return body.doOnNext(buffer -> {
            if (capped[0]) {
                return;
            }
            // toString(Charset) 按 position 读，不改 readerIndex：下游读到的还是完整的一份
            captured.append(buffer.toString(StandardCharsets.UTF_8));
            if (captured.length() > CAPTURE_LIMIT_CHARS) {
                capped[0] = true;
            }
        });
    }

    private static void noteStatus(int[] slot, HttpStatusCode status) {
        if (slot[0] < 0 && status != null) {
            slot[0] = status.value();
        }
    }

    private void record(ServerWebExchange exchange, String captured, int statusAtFirstWrite) {
        if (statusAtFirstWrite < 200 || statusAtFirstWrite >= 300) {
            // 被拒的（401/403/429）与首字节前就失败的（504）不产生 usage：写出去的是错误正文，
            // 厂商那侧根本没有这次调用
            return;
        }
        Usage usage = extract(captured);
        String tenant = tenantOf(exchange);
        if (usage == null) {
            counter("absent", tenant, exchange).increment();
            return;
        }
        counter("present", tenant, exchange).increment();
        tokenCounter("prompt", tenant, exchange).increment(usage.promptTokens());
        tokenCounter("completion", tenant, exchange).increment(usage.completionTokens());
        tokenCounter("cached", tenant, exchange).increment(usage.cachedTokens());
        tokenCounter("reasoning", tenant, exchange).increment(usage.reasoningTokens());
    }

    private Counter counter(String outcome, String tenant, ServerWebExchange exchange) {
        return Counter.builder(USAGE_SAMPLES_METRIC)
                .description("厂商有没有回 usage")
                .tag("outcome", outcome)
                .tag("tenant", tenant)
                .tag("provider", providerOf(exchange))
                .register(registry);
    }

    private Counter tokenCounter(String type, String tenant, ServerWebExchange exchange) {
        return Counter.builder(USAGE_TOKENS_METRIC)
                .description("token 用量，按类型分条")
                .tag("type", type)
                .tag("tenant", tenant)
                .tag("provider", providerOf(exchange))
                .register(registry);
    }

    private static String tenantOf(ServerWebExchange exchange) {
        Object tenant = exchange.getAttributes().get(TenantAuthFilter.TENANT_ATTRIBUTE);
        return tenant == null ? "anonymous" : tenant.toString();
    }

    private static String providerOf(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        return route == null ? "none" : String.valueOf(route.getMetadata().get("provider"));
    }

    /**
     * 逐条记录扫，取第一条带 {@code completion_tokens} 的。流式（每行一个 {@code data:} 帧）与
     * 非流式（整个正文一条 JSON）走同一段代码——差别只是有没有行前缀。
     *
     * <p>单条解析失败就跳过继续：分块可能把一帧拆坏，也可能厂商在 SSE 里塞了注释行（以 {@code :}
     * 开头）。采不到是 absent，不是异常。
     */
    static Usage extract(String body) {
        if (body.isEmpty()) {
            return null;
        }
        for (String line : body.split("\n")) {
            String record = line.strip();
            if (record.isEmpty() || record.startsWith(":")) {
                continue;
            }
            if (record.startsWith("data:")) {
                record = record.substring("data:".length()).strip();
            }
            if (record.isEmpty() || "[DONE]".equals(record) || !record.contains("completion_tokens")) {
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
}
