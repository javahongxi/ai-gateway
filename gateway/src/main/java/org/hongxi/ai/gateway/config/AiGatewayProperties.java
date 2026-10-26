package org.hongxi.ai.gateway.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关自己的配置段，与路由表并列放在 Nacos 的 ai-gateway.yaml 里。
 *
 * <p>为什么是可变形 JavaBean 而不是 record：这个 bean 要在 Nacos 改配置后被
 * spring-cloud-context 的 {@code ConfigurationPropertiesRebinder} 重新绑定，而它是
 * {@code Binder.bind(prefix, Bindable.ofInstance(bean))}——**往已有实例里灌值**。构造器绑定的
 * record 没有可灌的属性，重绑等于什么都没做。实测：把 record 版挂上去，环境里
 * {@code ai-gateway.tenants.team-a.api-key} 确实变了（日志有 refresh keys changed），但
 * bean 里还是旧 key，新 key 401、旧 key 200。SCG 自己的 {@code GatewayProperties} 就是
 * JavaBean，所以路由能热更、租户不能——差别在这个形状上。
 *
 * <p>租户查找由 {@code TenantAuthFilter} 做一次线性扫描：租户数量是几十这个量级，线性扫描
 * 比维护一份会随刷新失效的索引更可靠。
 */
@ConfigurationProperties(prefix = "ai-gateway")
public class AiGatewayProperties {

    /** 租户名到其属性的映射，键只是给人看的标识。 */
    private Map<String, Tenant> tenants = new LinkedHashMap<>();

    public Map<String, Tenant> getTenants() {
        return tenants;
    }

    public void setTenants(Map<String, Tenant> tenants) {
        this.tenants = tenants;
    }

    /** 一个可调用网关的租户。 */
    public static class Tenant {

        /** 颁发给租户的网关 key；上游厂商 key 不出现在这里。 */
        private String apiKey;

        /**
         * 允许访问的厂商别名，对应路由 metadata 里的 {@code provider}；为空表示不限制。
         */
        private List<String> providers = new ArrayList<>();

        public String getApiKey() {
            return apiKey;
        }

        public void setApiKey(String apiKey) {
            this.apiKey = apiKey;
        }

        public List<String> getProviders() {
            return providers;
        }

        public void setProviders(List<String> providers) {
            this.providers = providers;
        }

        public boolean allows(String provider) {
            return providers == null || providers.isEmpty() || providers.contains(provider);
        }
    }
}
