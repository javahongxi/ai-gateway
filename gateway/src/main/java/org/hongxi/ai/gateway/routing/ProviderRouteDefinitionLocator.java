package org.hongxi.ai.gateway.routing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Flux;

import org.springframework.cloud.gateway.route.RouteDefinitionLocator;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * 从 {@code ai-gateway.providers} 声明生成路由（决策 D10）。这一段替代的是原先手写在
 * {@code application.yml} 里的六条路由：两家厂商各一条对外、一条点名前缀、一条内部备份。
 *
 * <p><b>为什么值得多写这层。</b>手写表里真正脆的不是行数，是三条只能靠人记的规则：路由顺序就是
 * 优先级（专名在前、兜底在后，D7）、{@code CircuitBreaker} 必须排在 {@code SetPath} 与
 * {@code SetRequestHeader} 之前（否则改道重投递时鉴权读到厂商 key，直接 401，D6.1 实测踩过）、
 * 备份路由自己不挂熔断。留在 yml 里这些是约定；进代码就是能被单测钉住的行为。厂商从两家变四家时，
 * 手写是 12 条乘顺序乘 {@code forward:} 字符串，而声明只是多写一段。
 *
 * <p><b>产出的路由 id 与迁移前一致</b>（{@code {alias}-by-model} / {@code {alias}-chat} /
 * {@code {alias}-chat-direct} / {@code backup-{alias}}），指标与仪表板不用跟着改。对外入口是
 * {@code /v1/chat/completions}，点名厂商是 {@code /{alias}/v1/chat/completions}，备份入口带
 * {@code /backup/} 前缀且不该被客户端直接打。
 *
 * <p><b>顺序：手写优先，生成兜底。</b>spring-cloud-gateway 在 {@code CachingRouteLocator:65} 上按
 * {@code AnnotationAwareOrderComparator} 给路由排序，读的是每条路由自己的 {@code order}，与各
 * {@code RouteDefinitionLocator} 的先后无关（这一点是这次实测出来的，见决策 D10 的卡点①）。所以
 * 生成出来的路由统一 {@code order=1000}，而 {@code spring...routes} 里手写的默认是 0——手写那条
 * 永远先匹配。于是配置段既是迁移期的<b>逃生舱</b>（临时手写一条顶掉某条生成路由是有效手段），也让
 * 测试里注入的路由不会被真实厂商的生成路由抢走。
 *
 * <p><b>声明写错时不炸整张表。</b>缺 {@code base-url} 或 {@code chat-path} 的厂商被跳过并打
 * ERROR；{@code backup} 指到不存在的别名时，那条路由照常生成但不挂熔断——同样是错，但让全部厂商的
 * 路由在一次配置刷新里消失，代价是线上的。这类拓扑错误该由单测和启动日志负责，而不是由流量负责。
 */
@Component
public class ProviderRouteDefinitionLocator implements RouteDefinitionLocator {

    /** 生成的路由排在手写路由之后：见类注释「手写优先，生成兜底」。 */
    static final int GENERATED_ROUTE_ORDER = 1000;

    private static final Log log = LogFactory.getLog(ProviderRouteDefinitionLocator.class);

    /** 对外的 OpenAI 兼容入口。 */
    static final String CHAT_ENTRY = "/v1/chat/completions";

    /** 备份入口的前缀，客户端不该直接打；要点名厂商用 {@code /{alias}/v1/...}。 */
    static final String BACKUP_PREFIX = "/backup/";

    private final AiGatewayProperties properties;

    public ProviderRouteDefinitionLocator(AiGatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * 每次都按当前声明重算，不缓存。Nacos 改完 providers 之后 {@code RouteRefreshListener} 会触发
     * 路由重建，这里若留一份缓存，热更就成了看运气。
     */
    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        if (!properties.isGeneratedRoutes()) {
            // 关掉时一条都不产出，配置面就完全交给手写的 spring...routes（见 AiGatewayProperties
            // 里 generatedRoutes 的注释：测试类自带路由表，生产是急停闸）
            return Flux.empty();
        }
        return Flux.fromIterable(build(properties.getProviders()));
    }

    /** 包级可见：让单测能直接拿声明算出路由，不必起一个上下文。 */
    List<RouteDefinition> build(Map<String, AiGatewayProperties.Provider> providers) {
        if (providers == null || providers.isEmpty()) {
            // 没声明 providers 就是不用生成器：测试类们各自注入 routes，这段为空对它们没有影响
            return List.of();
        }
        List<RouteDefinition> out = new ArrayList<>();
        // 专名在前、兜底在后：stable sort 保留声明顺序，只把「有 models 的」整体提到前面
        List<Map.Entry<String, AiGatewayProperties.Provider>> ordered = new ArrayList<>(providers.entrySet());
        ordered.sort(Comparator.comparingInt(entry -> entry.getValue().getModels().isEmpty() ? 1 : 0));

        for (Map.Entry<String, AiGatewayProperties.Provider> entry : ordered) {
            String alias = entry.getKey();
            AiGatewayProperties.Provider provider = entry.getValue();
            if (!StringUtils.hasText(provider.getBaseUrl()) || !StringUtils.hasText(provider.getChatPath())) {
                log.error("provider '" + alias + "' 缺 base-url 或 chat-path，它的路由被跳过——"
                        + "这个厂商现在不可达，其余路由不受影响");
                continue;
            }
            boolean claimsModels = !provider.getModels().isEmpty();
            String publicId = alias + (claimsModels ? "-by-model" : "-chat");

            // 对外入口：不点名时由 Model 谓词按客户端传的模型名分流
            RouteDefinition shared = route(publicId, provider, alias);
            addEntryPredicates(shared, claimsModels ? List.of(entryPredicate(alias, provider)) : List.of());
            shared.setFilters(publicFilters(provider, alias, providers));
            out.add(shared);

            // 点名厂商：把 base_url 换成 .../{alias}/v1 就直接指定，不看 model 字段
            RouteDefinition direct = route(alias + "-chat-direct", provider, alias);
            direct.getPredicates().add(predicate("Path=/" + alias + CHAT_ENTRY));
            direct.setFilters(publicFilters(provider, alias, providers));
            out.add(direct);

            // 备份路由：只被 forward 命中，自己不再挂熔断——它就是最后那道防线。
            // 但它照样要过一遍模型名改写在（D9）：改道时熔断过滤器是在**它自己拿到的那个
            // exchange** 上重新分发的（SpringCloudCircuitBreakerFilterFactory:144），请求体里
            // 还是主厂商的名字，不在这一步换掉就会撞上「The supported API model names are …」。
            RouteDefinition backup = route("backup-" + alias, provider, alias);
            backup.getPredicates().add(predicate("Path=" + BACKUP_PREFIX + alias + CHAT_ENTRY));
            backup.setFilters(List.of(mapping(alias), setPath(provider), setKey(provider)));
            out.add(backup);
        }
        return out;
    }

    private RouteDefinition route(String id, AiGatewayProperties.Provider provider, String alias) {
        RouteDefinition definition = new RouteDefinition();
        definition.setId(id);
        definition.setUri(java.net.URI.create(provider.getBaseUrl()));
        Map<String, Object> metadata = new LinkedHashMap<>();
        // provider 这个键是全链路的单一事实源：厂商维度限流、指标 tag、D9 的模型名映射都读它
        metadata.put("provider", alias);
        metadata.put("response-timeout", provider.getResponseTimeout());
        metadata.put("connect-timeout", provider.getConnectTimeout());
        definition.setMetadata(metadata);
        definition.setOrder(GENERATED_ROUTE_ORDER);
        return definition;
    }

    private List<FilterDefinition> publicFilters(AiGatewayProperties.Provider provider, String alias,
            Map<String, AiGatewayProperties.Provider> providers) {
        List<FilterDefinition> filters = new ArrayList<>();
        FilterDefinition breaker = circuitBreaker(provider, alias, providers);
        if (breaker != null) {
            // 顺序有讲究：熔断要排在改路径与换 key 之前。它改道时用的是「自己看到的那个 request」，
            // 厂商 key 若已在前面被换进去，重投递时鉴权读到的就是厂商 key，直接 401（D6.1 实测踩过）。
            filters.add(breaker);
        }
        // 改写在熔断之后：它要按**最终命中**的那条路由的 provider 来定名字。改道会重走一遍分发，
        // 备用路由自己的这一步会用备用厂商的映射，见决策 D9。
        filters.add(mapping(alias));
        filters.add(setPath(provider));
        filters.add(setKey(provider));
        return filters;
    }

    private FilterDefinition circuitBreaker(AiGatewayProperties.Provider provider, String alias,
            Map<String, AiGatewayProperties.Provider> providers) {
        String backup = provider.getBackup();
        if (!StringUtils.hasText(backup)) {
            return null;
        }
        if (!providers.containsKey(backup)) {
            log.error("provider '" + alias + "' 的 backup 指向未声明的 '" + backup
                    + "'，这条路由本次不挂熔断（其余属性照常生成）");
            return null;
        }
        FilterDefinition definition = new FilterDefinition();
        definition.setName("CircuitBreaker");
        definition.addArg("name", alias);
        definition.addArg("fallbackUri", "forward:" + BACKUP_PREFIX + backup + CHAT_ENTRY);
        // 索引形式与手写 yml 里那条一致：args 会被 ConfigurationService 当属性源绑定到 Set<String>
        definition.addArg("statusCodes[0]", "INTERNAL_SERVER_ERROR");
        return definition;
    }

    private FilterDefinition mapping(String alias) {
        // 显式写 provider 而不是让过滤器去猜：路由自解释，读 /actuator 的人也看得出这条路由会
        // 用哪张映射表。改道之后备用路由带的是它自己那个别名，所以「跟最终路由走」这件事仍然成立。
        FilterDefinition definition = new FilterDefinition();
        definition.setName("ModelMapping");
        definition.addArg("provider", alias);
        return definition;
    }

    private FilterDefinition setPath(AiGatewayProperties.Provider provider) {
        FilterDefinition definition = new FilterDefinition();
        definition.setName("SetPath");
        // 键名是 SetPath 的 Config 字段名（SetPathGatewayFilterFactory.TEMPLATE_KEY），不是
        // "originalPath"——写错的话上下文启动时直接 NPE，这条是这次被测试逼出来的
        definition.addArg("template", provider.getChatPath());
        return definition;
    }

    private FilterDefinition setKey(AiGatewayProperties.Provider provider) {
        FilterDefinition definition = new FilterDefinition();
        definition.setName("SetRequestHeader");
        definition.addArg("name", "Authorization");
        definition.addArg("value", "Bearer " + provider.getKeyRef());
        return definition;
    }

    private PredicateDefinition entryPredicate(String alias, AiGatewayProperties.Provider provider) {
        // 专名路由的模型名匹配走自研的 Model 谓词（D7：内置 ReadBody 只能塞 SpEL）
        return predicate("Model=" + String.join(",", provider.getModels()));
    }

    private static PredicateDefinition predicate(String text) {
        return new PredicateDefinition(text);
    }

    private static void addEntryPredicates(RouteDefinition definition, List<PredicateDefinition> extra) {
        definition.getPredicates().add(predicate("Path=" + CHAT_ENTRY));
        definition.getPredicates().addAll(extra);
    }
}
