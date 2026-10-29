package org.hongxi.ai.gateway.observability;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.hongxi.ai.gateway.routing.ModelMappingGatewayFilterFactory;
import org.hongxi.ai.gateway.routing.ModelRoutePredicateFactory;

/**
 * R13：一行一个 JSON 的访问日志。它是这张链路上唯一能把「谁、打哪家、用哪个模型名、结果如何、
 * 用了多少 token」一次看全的地方。
 *
 * <p>指标看板回答趋势，trace 回答一次调用在系统里的路径，而排障第一天问的是<b>这一条请求</b>：
 * 那个租户、那个模型名、那次改道。这些在指标里是标签基数，在 trace 里是别的服务的 span，只有在
 * 一行日志里它们同时出现。所以这一行是"落一行、可 grep、字段固定"，不是结构化到需要查询引擎。
 *
 * <p>{@code requested_model} 与 {@code upstream_model} 是两个字段而不是一个：改道之后客户端传的
 * 名字和真正上线的名字不是一家（D9）。只记后者会让"客户端传的模型没人认领"这类 404 对不上号。
 *
 * <p><b>traceId 这一格目前是空的，除非客户端自己带了 {@code traceparent}。</b>这不是漏了：WebFlux
 * + OTel bridge 下 MDC 里没有 traceId（实测与原因见 docs 决策 D8），要填它得先补那条缺口，或者从
 * Reactor Context 里取当前 span。这里选择如实印空，而不是先印一个可能压根不存在关联关系的值。
 *
 * <p>顺序在 {@code RequestMetricsFilter} 之后、鉴权之前（鉴权是 {@code +10}）：被 401 / 403
 * 挡掉的请求也要有一行，那恰恰是最需要看日志的一类请求。
 */
@Component
public class AccessLogFilter implements GlobalFilter, Ordered {

    /** logger 名。运维要单独关或单独收集这一路日志，靠它。 */
    public static final String ACCESS_LOGGER = "ai.gateway.access";

    /** 厂商回自己请求 id 的头名，各家不统一，按顺序找第一个有的。 */
    private static final List<String> UPSTREAM_REQUEST_ID_HEADERS =
            List.of("x-request-id", "request-id", "x-requestid", "x-amzn-requestid", "x-dashscope-request-id");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger access = LoggerFactory.getLogger(ACCESS_LOGGER);

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 1;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long started = System.nanoTime();
        StringBuilder captured = new StringBuilder();
        boolean[] capped = {false};
        int[] statusAtFirstWrite = {-1};
        // 首字时刻也自己测，不从 FirstTokenMetricsFilter 的属性里读：那层在我们内侧、中间已经
        // mutate 过两次，实测内层写的属性在外层的 doFinally 里读不到（与 usage 同一个问题）。
        java.util.concurrent.atomic.AtomicLong firstByteAt = new java.util.concurrent.atomic.AtomicLong(-1L);

        // 日志自己看一遍响应字节，而不是等内层过滤器把结果传出来：exchange 每被 mutate 一次，
        // 属性表是不是同一份就不归本类管了（实测：用量过滤器确实采到了，日志读到的属性却是空）。
        // 与 UsageRecordingFilter 各 peek 一次，代价是同一份几 KB 的正文被读两遍，换来的是
        // 两层之间零耦合——扫描规则本身仍然只有一份（UsageRecordingFilter.extract）。
        ServerHttpResponse decorated = new ServerHttpResponseDecorator(exchange.getResponse()) {

            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                note(statusAtFirstWrite, getStatusCode());
                firstByteAt.compareAndSet(-1L, System.nanoTime());
                return super.writeWith(UsageRecordingFilter.peekBytes(Flux.from(body), captured, capped));
            }

            @Override
            public Mono<Void> writeAndFlushWith(
                    org.reactivestreams.Publisher<? extends org.reactivestreams.Publisher<? extends DataBuffer>> body) {
                note(statusAtFirstWrite, getStatusCode());
                firstByteAt.compareAndSet(-1L, System.nanoTime());
                Flux<Flux<DataBuffer>> nested = Flux.from(body)
                        .map(inner -> UsageRecordingFilter.peekBytes(Flux.from(inner), captured, capped));
                return super.writeAndFlushWith(nested);
            }
        };

        // doFinally 而不是 map：被鉴权短路、超时、客户端提前挂断，都要留下一行
        return chain.filter(exchange.mutate().response(decorated).build())
                .doFinally(signal -> write(exchange, started, signal.name(), captured.toString(),
                        statusAtFirstWrite[0], firstByteAt.get()));
    }

    private static void note(int[] slot, HttpStatusCode status) {
        if (slot[0] < 0 && status != null) {
            slot[0] = status.value();
        }
    }

    private void write(ServerWebExchange exchange, long startedNanos, String signal, String captured,
            int statusAtFirstWrite, long firstByteNanos) {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts", java.time.Instant.now().toString());
        line.put("tenant", tenantOf(exchange));
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        line.put("route", route == null ? "none" : route.getId());
        line.put("provider", route == null ? "none" : String.valueOf(route.getMetadata().get("provider")));
        line.put("outcome", signal);

        // 两个字段各查各的来源：upstream 只有走过改写才有值，requested 在没挂改写的路由上退回到
        // Model 谓词解析出来的那个名字（同一次请求它只解析一遍）
        String upstream = ModelMappingGatewayFilterFactory.upstreamModel(exchange);
        String requested = ModelMappingGatewayFilterFactory.requestedModel(exchange);
        if (requested == null) {
            requested = ModelRoutePredicateFactory.parsedModel(exchange);
        }
        line.put("requestedModel", requested);
        line.put("upstreamModel", upstream == null ? requested : upstream);

        HttpStatusCode status = exchange.getResponse().getStatusCode();
        line.put("status", status == null ? 0 : status.value());
        line.put("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos));
        if (firstByteNanos >= 0) {
            line.put("ttftMs", TimeUnit.NANOSECONDS.toMillis(firstByteNanos - startedNanos));
        }
        // 手工展开成 map 而不是直接把 record 交给 Jackson：默认 ObjectMapper 对 record 的
        // 规范构造器支持不牢靠（会序列化成数组或干脆抛异常），而这行的字段名是要被人 grep 的
        UsageRecordingFilter.Usage usage = statusAtFirstWrite >= 200 && statusAtFirstWrite < 300
                ? UsageRecordingFilter.extract(captured) : null;
        line.put("usage", usage == null ? "unknown" : usageMap(usage));

        // 字面量而不是 HttpHeaders.TRACE_PARENT：当前 Spring 版本没有这个常量，而 W3C 的头名
        // 本来就大小写不敏感，是 "traceparent"
        String traceparent = exchange.getRequest().getHeaders().getFirst("traceparent");
        if (traceparent != null) {
            line.put("traceparent", traceparent);
        }
        String upstreamId = upstreamRequestId(exchange);
        if (upstreamId != null) {
            line.put("upstreamRequestId", upstreamId);
        }

        try {
            // 单行：MAPPER 默认不加换行，日志采集按行切不会把它拆成两条
            access.info(MAPPER.writeValueAsString(line));
        }
        catch (Exception serializationFailed) {
            access.info("ai.gateway.access line unavailable: {}", serializationFailed.getClass().getName());
        }
    }

    private static Map<String, Object> usageMap(UsageRecordingFilter.Usage usage) {
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
}
