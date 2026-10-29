package org.hongxi.ai.gateway.routing;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.server.ServerWebExchange;

/**
 * 在过滤器里读请求体的那个 {@code model} 字段（R17 的模型白名单要用），顺带把"读过之后还能不能再
 * 把同一个体交下去"这件事一次做对。
 *
 * <p>两个坑都在这里，都不是显而易见：
 * <ol>
 *   <li>{@code ServerWebExchangeUtils.cacheRequestBody}（过滤器版）只把字节存进属性，<b>不换
 *       request 对象</b>——只有谓词用的 {@code cacheRequestBodyAndRequest} 会把 decorator 也存下来。
 *       所以读完之后如果不自己交回一个可重放的请求，下游 {@code NettyRoutingFilter} 拿到的是空流，
 *       厂商就一直等正文直到超时。</li>
 *   <li>要重放就必须重放<b>原始字节</b>。把 body 反序列化成 Map 再序列化回去，字段顺序与空白都可能
 *       变，那就等于网关改了报文——决策 D1 明确不许。</li>
 * </ol>
 *
 * <p>因此本类只读 {@code byte[]}，解析时另开一份，交下去的永远是原来那串字节。{@code ModelMapping}
 * 也用它，两处共用一份实现。
 */
public final class RequestBodyModels {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final List<org.springframework.http.codec.HttpMessageReader<?>> READERS =
            HandlerStrategies.withDefaults().messageReaders();

    private RequestBodyModels() {
    }

    /** 读到的模型名 + 应当继续往下传的 exchange（可能带着可重放的请求）。 */
    public record Resolved(String model, ServerWebExchange continued) {
    }

    /**
     * 取本请求的 {@code model}。已经有谓词解析过的直接复用；没有 JSON 正文的（GET、无 body）返回
     * {@code null} 且不碰流；其余情况缓存并重放。
     */
    public static Mono<Resolved> resolve(ServerWebExchange exchange) {
        String parsed = ModelRoutePredicateFactory.parsedModel(exchange);
        if (parsed != null && !parsed.isEmpty()) {
            return Mono.just(new Resolved(parsed, exchange));
        }
        if (!isJson(exchange)) {
            return Mono.just(new Resolved(null, exchange));
        }
        return ServerWebExchangeUtils.cacheRequestBody(exchange, request ->
                        ServerRequest.create(exchange.mutate().request(request).build(), READERS)
                                .bodyToMono(byte[].class)
                                .defaultIfEmpty(new byte[0]))
                .map(bytes -> {
                    return new Resolved(modelOf(bytes), replay(exchange, bytes));
                });
    }

    /** 从字节里挑出顶层 model；不是合法 JSON、不是对象、没有该字段都返回 null（由调用方决定怎么处置）。 */
    public static String modelOf(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            JsonNode root = MAPPER.readTree(bytes);
            JsonNode model = root.path("model");
            return model.isTextual() ? model.asText() : null;
        }
        catch (Exception unparsable) {
            return null;
        }
    }

    /**
     * 交回一个可读第二遍的请求体，字节与原文完全一致。{@code Content-Length} 按实际长度写一遍，
     * 免得改过体的情况下厂商侧截断。
     */
    public static ServerWebExchange replay(ServerWebExchange exchange, byte[] bytes) {
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        ServerHttpRequest decorated = new ServerHttpRequestDecorator(exchange.getRequest()) {

            @Override
            public Flux<DataBuffer> getBody() {
                return Flux.just(buffer);
            }

            @Override
            public HttpHeaders getHeaders() {
                HttpHeaders headers = new HttpHeaders();
                headers.putAll(super.getHeaders());
                headers.setContentLength(bytes.length);
                return headers;
            }
        };
        return exchange.mutate().request(decorated).build();
    }

    private static boolean isJson(ServerWebExchange exchange) {
        MediaType contentType = exchange.getRequest().getHeaders().getContentType();
        return contentType != null && MediaType.APPLICATION_JSON.isCompatibleWith(contentType);
    }
}
