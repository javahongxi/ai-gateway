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

    /**
     * 厂商声明。非空时由 {@code ProviderRouteDefinitionLocator} 生成路由，见决策 D10：
     * 备份关系、过滤器顺序这些规则进代码就能被单测钉住，留在 yml 里只能靠人记。
     */
    private Map<String, Provider> providers = new LinkedHashMap<>();

    /**
     * 是否由 {@code providers} 声明生成路由。生产保持默认 {@code true}；测试类自带路由表时关掉它
     * ——否则声明里那个不认领模型名的兜底厂商会抢走「没人认领的模型该 404」这类断言。
     * 这个开关也是运维用的急停闸：生成器出了问题时一行配置就能退回手写路由。
     */
    private boolean generatedRoutes = true;

    /**
     * 连续多少次语义失败（R14：HTTP 200 但正文里是限流/欠费/审核拒绝）就把该厂商的熔断直接切到
     * OPEN。中间出现一次正常响应就重新计数，所以它量的是"连续"而不是"累计"。
     *
     * <p>为什么不往熔断的滑动窗口里补记一次 {@code onError}：那台状态机已经把这次请求记成了一次
     * success，再塞一个事件等于同一发请求占两个位置、失败率立刻 50%，一发就能开闸。语义失败是
     * 另一种信号，就自己数、自己开闸。
     */
    private int semanticFailureThreshold = 3;

    /** 两个维度的配额。 */
    private RateLimit rateLimit = new RateLimit();

    public Map<String, Tenant> getTenants() {
        return tenants;
    }

    public void setTenants(Map<String, Tenant> tenants) {
        this.tenants = tenants;
    }

    public boolean isGeneratedRoutes() {
        return generatedRoutes;
    }

    public void setGeneratedRoutes(boolean generatedRoutes) {
        this.generatedRoutes = generatedRoutes;
    }

    public Map<String, Provider> getProviders() {
        return providers;
    }

    public void setProviders(Map<String, Provider> providers) {
        this.providers = providers;
    }

    public int getSemanticFailureThreshold() {
        return semanticFailureThreshold;
    }

    public void setSemanticFailureThreshold(int semanticFailureThreshold) {
        this.semanticFailureThreshold = semanticFailureThreshold;
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

    /**
     * 一个厂商的声明。这些字段合起来就是原先手写在路由表里的那六条，出处见决策 D10；
     * 生成器产出的路由 id 与迁移前一致，指标与仪表板不用改。
     *
     * <p>同样是可变形 JavaBean（理由见类注释 D4）：整段都靠 Nacos 热更，改 key 或改映射不该重启。
     */
    public static class Provider {

        /** 厂商根地址，例如 {@code https://dashscope.aliyuncs.com}。 */
        private String baseUrl;

        /**
         * 该厂商的 OpenAI 兼容 chat 端点路径，路由的 {@code SetPath} 就用它。
         * 与对外入口路径可以不同（DashScope 多一段 {@code /compatible-mode}）。
         */
        private String chatPath;

        /**
         * 上游凭证。配置里写 {@code ${DASHSCOPE_API_KEY}}，绑定时就解析完了——生成器不需要
         * 知道占位符这件事。
         */
        private String keyRef;

        /**
         * 这个厂商认领的模型名通配（对应 {@code Model} 谓词）。为空表示它是兜底厂商，
         * 生成时排在所有专名之后——路由顺序就是优先级，见 D7。
         */
        private List<String> models = new ArrayList<>();

        /** 等响应头的预算，毫秒。写进路由 metadata，只接受数字（D2）。 */
        private long responseTimeout = 120000;

        /** 建连预算，毫秒。 */
        private long connectTimeout = 3000;

        /**
         * 主厂商失败时改道去哪个厂商的别名；为空表示这条路由不挂熔断（备份路由自己就是最后
         * 那道防线）。
         */
        private String backup;

        /**
         * 客户端模型名到本厂商真实模型名的映射，决策 D9 的数据来源。表里没有的名字原样透传，
         * 不报错——未知模型让厂商自己回 4xx，网关不替它编名字。
         */
        private Map<String, String> modelMapping = new LinkedHashMap<>();

        /**
         * 这家厂商在 HTTP 200 的正文里自报失败时用的顶层错误码（限流、欠费、审核拒绝那一类）。
         * 只与响应的顶层 {@code code}、{@code error.code}、{@code error.type} 做大小写无关的
         * 等值比较——正文里的自由文本不参与判定。
         *
         * <p>每家有哪些码属于部署事实，本文档不替厂商定义；留空就是只依赖那条与厂商无关的结构
         * 规则（顶层 {@code error} 且无 {@code choices}）。
         */
        private List<String> failureIndicators = new ArrayList<>();

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public String getChatPath() {
            return chatPath;
        }

        public void setChatPath(String chatPath) {
            this.chatPath = chatPath;
        }

        public String getKeyRef() {
            return keyRef;
        }

        public void setKeyRef(String keyRef) {
            this.keyRef = keyRef;
        }

        public List<String> getModels() {
            return models;
        }

        public void setModels(List<String> models) {
            this.models = models;
        }

        public long getResponseTimeout() {
            return responseTimeout;
        }

        public void setResponseTimeout(long responseTimeout) {
            this.responseTimeout = responseTimeout;
        }

        public long getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public String getBackup() {
            return backup;
        }

        public void setBackup(String backup) {
            this.backup = backup;
        }

        public List<String> getFailureIndicators() {
            return failureIndicators;
        }

        public void setFailureIndicators(List<String> failureIndicators) {
            this.failureIndicators = failureIndicators;
        }

        public Map<String, String> getModelMapping() {
            return modelMapping;
        }

        public void setModelMapping(Map<String, String> modelMapping) {
            this.modelMapping = modelMapping;
        }
    }
}
