package org.hongxi.ai.gateway.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关自己的配置段，与路由表并列放在 Nacos 的 ai-gateway.yaml 里。
 *
 * <p>租户表用 {@code api-key -> tenant} 而不是 {@code tenant -> api-key}：请求进来时手里只有
 * key，反查一次比遍历整张表便宜。但配置刷新会把整个 record 换掉，所以这里保持声明式的
 * 「租户名 → 属性」形状，查找由 {@code TenantAuthFilter} 做一次线性扫描——租户数量是几十这个
 * 量级，线性扫描比维护一份会随刷新失效的索引更可靠。
 *
 * @param tenants 租户名到其属性的映射，键只是给人看的标识
 */
@ConfigurationProperties(prefix = "ai-gateway")
public record AiGatewayProperties(Map<String, Tenant> tenants) {

    /**
     * @param apiKey   颁发给租户的网关 key，上游厂商 key 不出现在这里
     * @param providers 允许访问的厂商别名，对应路由 metadata 里的 {@code provider}；
     *                  为空表示不限制
     */
    public record Tenant(String apiKey, List<String> providers) {

        public boolean allows(String provider) {
            return providers == null || providers.isEmpty() || providers.contains(provider);
        }
    }
}
