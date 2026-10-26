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

    /** 两个维度的配额。 */
    private RateLimit rateLimit = new RateLimit();

    public Map<String, Tenant> getTenants() {
        return tenants;
    }

    public void setTenants(Map<String, Tenant> tenants) {
        this.tenants = tenants;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(RateLimit rateLimit) {
        this.rateLimit = rateLimit;
    }

    /**
     * 限流配额。租户维度交给 spring-cloud-gateway 的 {@code RequestRateLimiter}（它写
     * {@code X-RateLimit-*} 头），厂商维度由 {@code ProviderQuotaFilter} 自己实现——原因见
     * docs/requirements.md 的决策 D5：同一条路由上挂两个 RequestRateLimiter，后一个的配额会把
     * 前一个覆盖掉。
     */
    public static class RateLimit {

        /** 租户桶：每秒补几个。 */
        private int tenantReplenishRate = 5;

        /** 租户桶：能攒几个。 */
        private int tenantBurstCapacity = 10;

        /** 租户桶：一次调用扣几个令牌。默认 1；把长回答记成几个令牌就调大。 */
        private int tenantRequestedTokens = 1;

        /**
         * 厂商桶：一个窗口内允许几次，0 表示关掉这一维。窗口长度是
         * {@link #providerWindowSeconds}。
         */
        private int providerRequestsPerWindow = 20;

        /**
         * 厂商窗口长度，秒。上游账户配额一般按分钟（RPM）给，而一次模型调用本身就要花几秒到
         * 几十秒——按秒开窗等于永远撞不到，所以这里默认 60。
         */
        private int providerWindowSeconds = 60;

        public int getTenantReplenishRate() {
            return tenantReplenishRate;
        }

        public void setTenantReplenishRate(int tenantReplenishRate) {
            this.tenantReplenishRate = tenantReplenishRate;
        }

        public int getTenantBurstCapacity() {
            return tenantBurstCapacity;
        }

        public void setTenantBurstCapacity(int tenantBurstCapacity) {
            this.tenantBurstCapacity = tenantBurstCapacity;
        }

        public int getTenantRequestedTokens() {
            return tenantRequestedTokens;
        }

        public void setTenantRequestedTokens(int tenantRequestedTokens) {
            this.tenantRequestedTokens = tenantRequestedTokens;
        }

        public int getProviderRequestsPerWindow() {
            return providerRequestsPerWindow;
        }

        public void setProviderRequestsPerWindow(int providerRequestsPerWindow) {
            this.providerRequestsPerWindow = providerRequestsPerWindow;
        }

        public int getProviderWindowSeconds() {
            return providerWindowSeconds;
        }

        public void setProviderWindowSeconds(int providerWindowSeconds) {
            this.providerWindowSeconds = providerWindowSeconds;
        }
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
