package org.hongxi.ai.gateway.observability;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
 * R9 的首字延迟（TTFT）：从请求进入网关到第一个响应字节交给客户端，单独记一个指标。
 *
 * <p>为什么整条请求的耗时不够用：模型调用真正决定体验的是「多久开始吐字」，一段两三千 token
 * 的回答可以跑一分钟，但那不是延迟，那是产出。只看 {@code ai.gateway.requests} 会把两件事混成
 * 一个数字，慢回答看起来像故障。
 *
 * <p>挂点在响应侧：{@code NettyWriteResponseFilter} 走的是
 * {@code exchange.getResponse().writeAndFlushWith(...)}（流式）或 {@code writeWith(...)}，
 * 所以只要排在它之前（order &lt; -1）把 exchange 的 response 换成装饰器，就能在字节真正交给
 * 客户端的那一刻记时——这是**客户端看到**的首字节，不是厂商返回的首字节，正是用户在意的那个。
 *
 * <p>只观察，不改动任何字节：与决策 D1 不冲突。
 */
@Component
public class FirstTokenMetricsFilter implements GlobalFilter, Ordered {

    /** 首字延迟指标名。 */
    public static final String TTFT_METRIC = "ai.gateway.ttft";

    private final MeterRegistry registry;

    public FirstTokenMetricsFilter(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public int getOrder() {
        // 必须早于 NettyWriteResponseFilter(-1)，否则它拿到的还是没装饰过的 response
        return -2;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        AtomicLong started = new AtomicLong(System.nanoTime());
        AtomicLong firstByteAt = new AtomicLong(-1L);
        ServerHttpResponse decorated = new ServerHttpResponseDecorator(exchange.getResponse()) {

            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {
                return super.writeWith(markFirst(Flux.from(body), firstByteAt));
            }

            @Override
            public Mono<Void> writeAndFlushWith(
                    org.reactivestreams.Publisher<? extends org.reactivestreams.Publisher<? extends DataBuffer>> body) {
                Flux<Flux<DataBuffer>> nested = Flux.from(body)
                        .map(inner -> markFirst(Flux.from(inner), firstByteAt));
                return super.writeAndFlushWith(nested);
            }
        };
        return chain.filter(exchange.mutate().response(decorated).build())
                .doFinally(signal -> record(exchange, started, firstByteAt));
    }

    private static Flux<DataBuffer> markFirst(Flux<DataBuffer> body, AtomicLong firstByteAt) {
        return body.doOnNext(buffer -> firstByteAt.compareAndSet(-1L, System.nanoTime()));
    }

    private void record(ServerWebExchange exchange, AtomicLong started, AtomicLong firstByteAt) {
        long first = firstByteAt.get();
        HttpStatusCode status = exchange.getResponse().getStatusCode();
        if (first < 0 || status == null || !status.is2xxSuccessful()) {
            // 只给真正开始出模型的响应记首字延迟：401 / 429 / 504 写出去的字节是错误正文，
            // 把它算进分位数会把指标拖歪
            return;
        }
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        Object tenant = exchange.getAttributes().get(TenantAuthFilter.TENANT_ATTRIBUTE);
        Timer.builder(TTFT_METRIC)
                .description("从进入网关到第一个响应字节交给客户端")
                .tag("tenant", tenant == null ? "anonymous" : tenant.toString())
                .tag("provider", route == null ? "none" : String.valueOf(route.getMetadata().get("provider")))
                .tag("route", route == null ? "none" : route.getId())
                .register(registry)
                .record(first - started.get(), TimeUnit.NANOSECONDS);
    }
}
