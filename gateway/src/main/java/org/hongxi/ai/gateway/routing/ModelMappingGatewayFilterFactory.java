package org.hongxi.ai.gateway.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.codec.HttpMessageReader;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * D9：请求侧的模型名改写。客户端传的是一个名字，厂商认的是自己那套名字，这一段只做这一件事。
 *
 * <p><b>只改 {@code model}，只在请求侧。</b>响应体永远不碰——那是 TTFT 与 R12 计量能成立的前提。
 * 改写是一次几 KB 的对象重建，发生在请求还没开始流之前，所以首字延迟不受影响。其余字段
 * （messages、采样参数、{@code stream_options}）一概不动，D1 那句「要注入字段就要求客户端自带」
 * 仍然有效。
 *
 * <p><b>为什么必须在 {@code CircuitBreaker} 之后、且跟着最终路由走。</b>熔断过滤器改道时是在
 * <em>它自己拿到的那个 exchange</em> 上重新分发的
 * （{@code SpringCloudCircuitBreakerFilterFactory:143-144}），也就是说备用路由的过滤器链会完整
 * 再跑一遍——包括这一步。所以每条路由只认自己那个 provider 的映射表（从路由 metadata 读，不需要
 * args），不需要在主路由上「预测改道后会落到哪家」，那等于把拓扑知识塞进过滤器。
 *
 * <p><b>体在二次分发里从哪来。</b>这是这件事唯一的难点：{@code cacheRequestBody} 会把上游请求的
 * body 流 join 掉，而它只在 exchange 的<b>属性</b>里留下缓存（{@code ServerWebExchangeUtils:396-402}
 * 有就短路），不改原 request 对象——改道重新分发用的还是那个 request，流已经空了。因此本过滤器把
 * 第一次读到的<b>原始字节</b>存在自己的属性里，第二次进入时从属性重算，不再碰那个已经被抽干的流。
 * {@code ServerWebExchangeUtils.reset()} 只清连接与响应相关属性、不动请求体缓存，这条路才成立。
 * 映射的键也一律用<b>原始</b>模型名（存在 {@code aiGatewayRequestedModel} 里），所以主厂商先把
 * {@code chat-default} 换成 {@code qwen3.7-plus} 之后，改道到备用厂商时仍然能拿到
 * {@code deepseek-flash}，而不是对着一个已经改过的名字无能为力。
 *
 * <p><b>两处保守：</b>映射表为空或请求不是 JSON 时直接放行，一个字节都不动——今天「逐字节透传」
 * 的行为不能被这个过滤器悄悄改掉；表里没有的模型名原样送出去，未知模型让厂商回它自己的 4xx，
 * 网关不替厂商编名字。
 */
@Component
public class ModelMappingGatewayFilterFactory
        extends AbstractGatewayFilterFactory<ModelMappingGatewayFilterFactory.Config> {

    /** 第一次读进来的原始字节，改道重投递时靠它重建请求体。 */
    static final String ORIGINAL_BODY_KEY = "aiGatewayOriginalBody";

    /** 客户端原本传的模型名，映射的键与 R13 日志要的 requested_model。 */
    static final String REQUESTED_MODEL_KEY = "aiGatewayRequestedModel";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<HttpMessageReader<?>> messageReaders = HandlerStrategies.withDefaults().messageReaders();

    private final AiGatewayProperties properties;

    public ModelMappingGatewayFilterFactory(AiGatewayProperties properties) {
        super(Config.class);
        this.properties = properties;
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return List.of("provider");
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            Map<String, String> mapping = mappingFor(exchange, config);
            if (mapping.isEmpty() || !isJsonRequest(exchange)) {
                return chain.filter(exchange);
            }
            byte[] already = exchange.getAttribute(ORIGINAL_BODY_KEY);
            if (already != null) {
                // 二次进入（改道重投递）：那个流已经空了，从属性里留的原始字节重建请求
                return chain.filter(mapName(exchange, already, mapping));
            }
            return ServerWebExchangeUtils.cacheRequestBody(exchange, request ->
                            ServerRequest.create(exchange.mutate().request(request).build(), messageReaders)
                                    .bodyToMono(byte[].class)
                                    .defaultIfEmpty(new byte[0]))
                    .map(body -> {
                        exchange.getAttributes().put(ORIGINAL_BODY_KEY, body);
                        return mapName(exchange, body, mapping);
                    })
                    .flatMap(rewritten -> chain.filter(rewritten))
                    .switchIfEmpty(Mono.defer(() -> chain.filter(exchange)));
        };
    }

    /**
     * 按映射表决定送出去的那个模型名。
     *
     * <p>不管改没改，返回的 exchange 都带一个<b>可重放的</b>请求体。这不是防御性冗余：
     * {@code ServerWebExchangeUtils.cacheRequestBody} 在 filter 这一侧只把字节存进属性、
     * 不换 request 对象（{@code cacheRequestBodyAndRequest} 才会缓存 decorator），所以上游那个流
     * 已经被 join 走了——不交回一个可重放的体，下游 {@code NettyRoutingFilter} 拿到的是空流，
     * 厂商就会一直等正文直到超时（这条是端到端用例第一次跑出来时 20 秒挂死给我的）。
     * 没命中映射时交回去的就是原字节，所以「逐字节透传」那条性质仍然成立，也有用例钉着。
     */
    private ServerWebExchange mapName(ServerWebExchange exchange, byte[] body, Map<String, String> mapping) {
        if (body.length == 0) {
            return RequestBodyModels.replay(exchange, body);
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        }
        catch (Exception unparsable) {
            // 不是合法 JSON 就不自作聪明：让厂商去报它该报的错，比网关猜一个形状更可信
            return RequestBodyModels.replay(exchange, body);
        }
        if (!root.isObject() || !root.path("model").isTextual()) {
            return RequestBodyModels.replay(exchange, body);
        }
        String inBody = root.path("model").asText();
        // 第一次进来时把客户端原始名字钉住，之后每一轮都用它当键（见类注释）
        String requested = exchange.getAttributes().containsKey(REQUESTED_MODEL_KEY)
                ? exchange.<String>getAttribute(REQUESTED_MODEL_KEY)
                : inBody;
        exchange.getAttributes().put(REQUESTED_MODEL_KEY, requested);
        String target = mapping.getOrDefault(requested, requested);
        // 就算这一次没换名字（target 与 inBody 相同），上线的也是它——日志那两个字段要的是事实
        // 不是"改过才记"
        exchange.getAttributes().put(UPSTREAM_MODEL_KEY, target);
        if (target.equals(inBody)) {
            return RequestBodyModels.replay(exchange, body);
        }

        try {
            ((ObjectNode) root).put("model", target);
            return RequestBodyModels.replay(exchange, MAPPER.writeValueAsBytes(root));
        }
        catch (Exception serializationFailed) {
            return RequestBodyModels.replay(exchange, body);
        }
    }

    /**
     * 取映射表：先看<b>本条路由</b> metadata 上的 {@code model-mapping}，再用该厂商的表。
     *
     * <p>这个优先级是 R20 逼出来的：同一个厂商可以在模型组的不同跳上以不同真实名出现，映射因此
     * 是<b>路由</b>的属性而不是厂商的属性（见 docs 决策 D17）。
     */
    private Map<String, String> mappingFor(ServerWebExchange exchange, Config config) {
        Map<String, String> onRoute = routeMapping(exchange);
        if (!onRoute.isEmpty()) {
            return onRoute;
        }
        String provider = StringUtils.hasText(config.getProvider())
                ? config.getProvider()
                : providerOf(exchange);
        if (provider == null) {
            return Map.of();
        }
        AiGatewayProperties.Provider declared = properties.getProviders().get(provider);
        if (declared == null || declared.getModelMapping() == null) {
            return Map.of();
        }
        return declared.getModelMapping();
    }

    /** 读路由 metadata 上的映射表；没有就是空表（不猜）。 */
    private static Map<String, String> routeMapping(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null || route.getMetadata() == null) {
            return Map.of();
        }
        Object raw = route.getMetadata().get(MODEL_MAPPING_METADATA_KEY);
        if (!(raw instanceof Map<?, ?> map)) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        map.forEach((k, v) -> out.put(String.valueOf(k), v == null ? null : String.valueOf(v)));
        return out;
    }

    private static String providerOf(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null || route.getMetadata() == null) {
            return null;
        }
        Object provider = route.getMetadata().get("provider");
        return provider == null ? null : provider.toString();
    }

    private static boolean isJsonRequest(ServerWebExchange exchange) {
        var contentType = exchange.getRequest().getHeaders().getContentType();
        return contentType != null && org.springframework.http.MediaType.APPLICATION_JSON.isCompatibleWith(contentType);
    }

    /** 改写真正上线后属性里放的键，与 requested 那个成一对（R13 日志要两个字段）。 */
    static final String UPSTREAM_MODEL_KEY = "aiGatewayUpstreamModel";

    /** 路由 metadata 上的映射表键（R20 的模型组用它给每一跳指定自己的真名）。 */
    public static final String MODEL_MAPPING_METADATA_KEY = "model-mapping";

    /** 客户端原本传的模型名（R13 的 requested_model）。 */
    public static String requestedModel(ServerWebExchange exchange) {
        return exchange.getAttribute(REQUESTED_MODEL_KEY);
    }

    /** 真正上线的模型名（R13 的 upstream_model）。没经过改写的请求为 null，由日志侧退化处理。 */
    public static String upstreamModel(ServerWebExchange exchange) {
        return exchange.getAttribute(UPSTREAM_MODEL_KEY);
    }

    /**
     * 配置形状：留空即按当前路由的 provider 取映射表（生成器就是这么用的）；显式写
     * {@code ModelMapping=deepseek} 则钉死用哪张表。
     */
    public static class Config {

        private String provider;

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        @Override
        public String toString() {
            return "ModelMapping: " + (provider == null ? "<route metadata>" : provider);
        }
    }
}
