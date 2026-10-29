package org.hongxi.ai.gateway.routing;

import java.util.List;

import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R16：请求阶段决定这次调用用哪家厂商的<b>哪把 key</b>，并把 {@code Authorization} 换上去。
 *
 * <p>它取代了生成路由里那条静态的 {@code SetRequestHeader=Authorization,Bearer xxx}——密钥池一旦
 * 多于一把，"用哪把"就不可能再是配置期的常量，必须是每次调用的决定。单把 key 时行为等价，只是
 * 换成了同一条路径，少一种"两种写法各跑各的"。
 *
 * <p>放在请求阶段（不是完成阶段）有两个理由：一是它必须早于厂商维度限流，那个窗口要按
 * {@code provider:key} 分桶；二是选中的 key 要能被外层的访问日志读到，而<b>内层在
 * {@code doFinally} 里写的属性外层读不到</b>（现象与探针见 docs 决策 D11），请求阶段写才安全。
 *
 * <p>没有可用凭证时原样放行，让厂商回它的 401：那是最容易被读到的信号，比网关自己编一个错误码
 * 更接近事实。
 */
@Component
public class ProviderKeyGatewayFilterFactory
        extends AbstractGatewayFilterFactory<ProviderKeyGatewayFilterFactory.Config> {

    private final AiGatewayProperties properties;

    private final UpstreamKeyPool keyPool;

    public ProviderKeyGatewayFilterFactory(AiGatewayProperties properties, UpstreamKeyPool keyPool) {
        super(Config.class);
        this.properties = properties;
        this.keyPool = keyPool;
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return List.of("provider");
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String provider = StringUtils.hasText(config.getProvider()) ? config.getProvider() : providerOf(exchange);
            String key = keyPool.pick(provider);
            if (key == null) {
                return chain.filter(exchange);
            }
            // 请求阶段写：日志与厂商窗口都读这两个属性
            exchange.getAttributes().put(UpstreamKeyPool.SELECTED_KEY_ATTRIBUTE, key);
            exchange.getAttributes().put(UpstreamKeyPool.SELECTED_FINGERPRINT_ATTRIBUTE,
                    UpstreamKeyPool.fingerprint(key));

            ServerHttpRequest mutated = exchange.getRequest().mutate()
                    .headers(headers -> headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + key))
                    .build();
            return chain.filter(exchange.mutate().request(mutated).build());
        };
    }

    private static String providerOf(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null || route.getMetadata() == null) {
            return null;
        }
        Object provider = route.getMetadata().get("provider");
        return provider == null ? null : provider.toString();
    }

    /** 配置形状：{@code ProviderKey=dashscope}；省略则取路由 metadata 的 provider。 */
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
            return "ProviderKey: " + (StringUtils.hasText(provider) ? provider : "<route metadata>");
        }
    }
}
