package org.hongxi.ai.gateway.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.handler.AsyncPredicate;
import org.springframework.cloud.gateway.handler.predicate.AbstractRoutePredicateFactory;
import org.springframework.http.codec.HttpMessageReader;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.server.ServerWebExchange;

/**
 * 按请求体里的 {@code model} 选路由：R3 的模型路由。
 *
 * <p>为什么自己写而不用现成的 {@code ReadBody}：那个谓词的 {@code predicate} 参数类型是
 * {@code java.util.function.Predicate}，SCG 里没有 String→Predicate 的转换器，配置里只能写成
 * {@code #{...}} 去引用一个 Java 侧的 bean——绕一圈还是得写 Java，却把匹配逻辑藏进一个 SpEL
 * 字符串里。而大模型网关的「按模型分流」是核心概念，值得有自己的谓词名：配置读起来就是
 * {@code - Model=deepseek*}。
 *
 * <p>读请求体的方式与 {@code ReadBody} 相同：{@code cacheRequestBodyAndRequest} 把 body 缓存下来
 * 并换一个可重读的 request 进 exchange，所以谓词读过之后，转发给厂商的正文一个字节都不少
 * （这条有专门的测试钉住）。代价是请求体要在网关内存里过一遍——聊天请求是几 KB 量级，
 * 而且这一步在响应之前，不影响流式首字延迟。
 */
@Component
public class ModelRoutePredicateFactory extends AbstractRoutePredicateFactory<ModelRoutePredicateFactory.Config> {

    /** exchange 上缓存的解析结果，避免同一个请求被多条候选路由反复反序列化。 */
    private static final String CACHED_MODEL_KEY = "aiGatewayModel";

    private final List<HttpMessageReader<?>> messageReaders;

    public ModelRoutePredicateFactory() {
        super(Config.class);
        this.messageReaders = HandlerStrategies.withDefaults().messageReaders();
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return List.of("patterns");
    }

    @Override
    public Predicate<ServerWebExchange> apply(Config config) {
        throw new UnsupportedOperationException("Model 谓词只支持异步形式（它要读请求体）");
    }

    @Override
    public AsyncPredicate<ServerWebExchange> applyAsync(Config config) {
        return exchange -> {
            Object cached = exchange.getAttributes().get(CACHED_MODEL_KEY);
            if (cached != null) {
                return Mono.just(matches(config, cached.toString()));
            }
            if (!isJsonRequest(exchange)) {
                // GET /v1/models 这类没有 JSON 正文的请求不该在这里被卡住
                return Mono.just(false);
            }
            return ServerWebExchangeUtils.cacheRequestBodyAndRequest(exchange, serverHttpRequest ->
                    ServerRequest.create(exchange.mutate().request(serverHttpRequest).build(), messageReaders)
                            .bodyToMono(Map.class)
                            .defaultIfEmpty(Map.of())
                            .doOnNext(body -> exchange.getAttributes().put(CACHED_MODEL_KEY,
                                    String.valueOf(body.getOrDefault("model", ""))))
                            .map(body -> matches(config, String.valueOf(body.getOrDefault("model", "")))));
        };
    }

    private static boolean isJsonRequest(ServerWebExchange exchange) {
        MediaType contentType = exchange.getRequest().getHeaders().getContentType();
        return contentType != null && MediaType.APPLICATION_JSON.isCompatibleWith(contentType);
    }

    /**
     * 通配匹配：{@code qwen*}、{@code *} 这类。用 Spring 的 {@link PatternMatchUtils}，
     * 它只认 {@code *} 前后缀与中间通配，不会把模型名当正则解析——模型名里有点号和冒号
     * （{@code deepseek-v4-flash:latest}），当正则迟早出事。
     */
    private static boolean matches(Config config, String model) {
        if (config.getPatterns().isEmpty()) {
            return true;
        }
        return PatternMatchUtils.simpleMatch(config.getPatterns().toArray(new String[0]), model);
    }

    /** 配置形状：{@code - name: Model}{@code  args: {patterns: [ "qwen*", "glm*" ]}}，或简写 {@code Model=qwen*,glm*}。 */
    public static class Config {

        private List<String> patterns = new ArrayList<>();

        public List<String> getPatterns() {
            return patterns;
        }

        public void setPatterns(List<String> patterns) {
            this.patterns = patterns;
        }

        @Override
        public String toString() {
            return "Model: " + patterns;
        }
    }
}
