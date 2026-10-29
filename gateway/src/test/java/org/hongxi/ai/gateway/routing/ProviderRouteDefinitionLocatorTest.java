package org.hongxi.ai.gateway.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 决策 D10 的生成器单测：不起上下文，直接把声明喂给 {@code build}，把「三条只能靠人记的规则」
 * 变成断言。这三条在迁移前是 yml 里的注释、靠自觉；现在谁改坏谁会立刻看到红的用例。
 *
 * <p>顺带钉住两件事：路由 id 与迁移前手写的那六条一致（指标与仪表板不用跟着改），以及
 * {@code providers} 为空时什么都不产出（其余测试类各自注入自己的 routes，不受生成器影响）。
 */
class ProviderRouteDefinitionLocatorTest {

    private static AiGatewayProperties.Provider provider(String baseUrl, String chatPath, String backup,
            List<String> models) {
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setBaseUrl(baseUrl);
        declared.setChatPath(chatPath);
        declared.setKeyRef(baseUrl == null ? "sk-none" : "sk-" + Math.abs(baseUrl.hashCode()));
        declared.setBackup(backup);
        declared.setModels(models);
        return declared;
    }

    /** 声明按写入顺序排，所以这里用 LinkedHashMap——「专名在前」的排序是生成器做的事，不是 Map 的事。 */
    private static Map<String, AiGatewayProperties.Provider> decl(Object... aliasThenProvider) {
        Map<String, AiGatewayProperties.Provider> out = new LinkedHashMap<>();
        for (int i = 0; i < aliasThenProvider.length; i += 2) {
            out.put((String) aliasThenProvider[i], (AiGatewayProperties.Provider) aliasThenProvider[i + 1]);
        }
        return out;
    }

    private static List<RouteDefinition> generated(Map<String, AiGatewayProperties.Provider> providers) {
        return new ProviderRouteDefinitionLocator(new AiGatewayProperties()).build(providers);
    }

    /** 两家厂商的常见形状：deepseek 认领专名，dashscope 是兜底。 */
    private static List<RouteDefinition> twoProviders(String deepseekBackup, String dashscopeBackup) {
        return generated(decl(
                "dashscope", provider("https://dashscope.aliyuncs.com", "/compatible-mode/v1/chat/completions",
                        dashscopeBackup, List.of()),
                "deepseek", provider("https://api.deepseek.com", "/v1/chat/completions", deepseekBackup,
                        List.of("deepseek*"))));
    }

    private static List<String> filterNames(RouteDefinition definition) {
        return definition.getFilters().stream().map(FilterDefinition::getName).toList();
    }

    @Test
    void producesTheSameRouteIdsTheHandwrittenTableHad() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        assertThat(routes).extracting(RouteDefinition::getId).containsExactly(
                // 声明里 dashscope 写在前，但它没有认领模型；专名厂商被生成器提到前面（D7：顺序就是优先级）
                "deepseek-by-model", "deepseek-chat-direct", "backup-deepseek",
                "dashscope-chat", "dashscope-chat-direct", "backup-dashscope");
    }

    /**
     * D6.1 那条实测踩出来的顺序：熔断必须在换路径与换 key 之前，否则改道重投递时鉴权读到的
     * 是厂商 key，直接 401。生成器把这个顺序写死，人就再也写错它。
     */
    @Test
    void circuitBreakerComesBeforeTheRewritesOnPublicRoutes() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        assertThat(filterNames(routes.get(0))).containsExactly("CircuitBreaker", "ModelMapping", "SetPath",
                "SetRequestHeader");
        assertThat(filterNames(routes.get(1))).containsExactly("CircuitBreaker", "ModelMapping", "SetPath",
                "SetRequestHeader");
        // 兜底厂商自己的对外路由同样带熔断（它也能改道去 deepseek）
        assertThat(filterNames(routeById(routes, "dashscope-chat"))).containsExactly("CircuitBreaker", "ModelMapping",
                "SetPath", "SetRequestHeader");
    }

    /** 备份路由是最后那道防线：自己不挂熔断，但要照样做名字改写（D9）。 */
    @Test
    void backupRoutesCarryMappingButNoBreaker() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        RouteDefinition backup = routeById(routes, "backup-dashscope");
        assertThat(filterNames(backup)).containsExactly("ModelMapping", "SetPath", "SetRequestHeader");
        assertThat(backup.getPredicates()).hasSize(1);
        assertThat(backup.getPredicates().get(0).getArgs()).containsValue("/backup/dashscope/v1/chat/completions");
    }

    @Test
    void publicEntryPathCarriesTheModelPredicateOnlyForNamedProviders() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        assertThat(routeById(routes, "deepseek-by-model").getPredicates())
                .extracting(PredicateDefinition::getName).containsExactly("Path", "Model");
        assertThat(routeById(routes, "deepseek-by-model").getPredicates().get(1).getName()).isEqualTo("Model");
        assertThat(routeById(routes, "deepseek-by-model").getPredicates().get(1).getArgs()).containsValue("deepseek*");
        // 兜底厂商不认领模型名：谁都没认领的名字才轮到它
        assertThat(routeById(routes, "dashscope-chat").getPredicates())
                .extracting(PredicateDefinition::getName).containsExactly("Path");
    }

    @Test
    void fallbackUriPointsAtTheDeclaredBackupProvider() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        FilterDefinition breaker = routes.get(0).getFilters().get(0);
        assertThat(breaker.getArgs()).containsEntry("name", "deepseek")
                .containsEntry("fallbackUri", "forward:/backup/dashscope/v1/chat/completions")
                .containsEntry("statusCodes[0]", "INTERNAL_SERVER_ERROR");
    }

    /** 路由 order=1000 是「手写优先，生成兜底」的全部机制，见类注释与 D10 卡点①。 */
    @Test
    void generatedRoutesSortBehindHandwrittenOnes() {
        assertThat(twoProviders(null, null)).allSatisfy(definition -> assertThat(definition.getOrder())
                .isEqualTo(ProviderRouteDefinitionLocator.GENERATED_ROUTE_ORDER));
    }

    @Test
    void metadataCarriesProviderAndNumericTimeouts() {
        List<RouteDefinition> routes = twoProviders(null, null);

        Map<String, Object> metadata = routeById(routes, "dashscope-chat").getMetadata();
        assertThat(metadata).containsEntry("provider", "dashscope");
        // D2：这两个键必须是数字，写成 "120s" 会被静默忽略或直接抛异常
        assertThat(metadata.get("response-timeout")).isInstanceOf(Long.class);
        assertThat(metadata.get("connect-timeout")).isInstanceOf(Long.class);
    }

    @Test
    void mappingFilterNamesTheProviderItWillUse() {
        List<RouteDefinition> routes = twoProviders("dashscope", "deepseek");

        FilterDefinition mapping = routeById(routes, "backup-deepseek").getFilters().get(0);
        assertThat(mapping.getName()).isEqualTo("ModelMapping");
        // 显式写别名而不是让过滤器去猜：改道后备用路由带的就是备用那个别名，
        // 「跟最终路由走」因此不需要谁去预测改道目标
        assertThat(mapping.getArgs()).containsEntry("provider", "deepseek");
    }

    /** 拓扑写错不该让整张表消失：降级那一条，其余照常，日志负责说话。 */
    @Test
    void unknownBackupDegradesToNoBreakerInsteadOfFailingTheTable() {
        List<RouteDefinition> routes = twoProviders("typo-alias", null);

        assertThat(filterNames(routeById(routes, "deepseek-by-model")))
                .containsExactly("ModelMapping", "SetPath", "SetRequestHeader");
        assertThat(routes).hasSize(6);
    }

    @Test
    void incompleteProviderIsSkippedNotHalfBuilt() {
        List<RouteDefinition> routes = generated(decl(
                "broken", provider(null, "/v1/chat/completions", null, List.of()),
                "dashscope", provider("https://dashscope.aliyuncs.com", "/compatible-mode/v1/chat/completions", null,
                        List.of())));

        assertThat(routes).extracting(RouteDefinition::getId)
                .containsExactly("dashscope-chat", "dashscope-chat-direct", "backup-dashscope");
    }

    @Test
    void noProvidersMeansNoGeneratedRoutes() {
        // 其余测试类各自注入 routes；生成器为空才不会跟它们抢路径
        assertThat(new ProviderRouteDefinitionLocator(new AiGatewayProperties()).getRouteDefinitions()
                .collectList().block()).isEmpty();
    }

    /** 无状态：刷新一次不翻倍（D10 卡点③）。 */
    @Test
    void buildingTwiceProducesTheSameListWithoutAccumulation() {
        Map<String, AiGatewayProperties.Provider> providers = decl(
                "deepseek", provider("https://api.deepseek.com", "/v1/chat/completions", null, List.of("deepseek*")),
                "dashscope", provider("https://dashscope.aliyuncs.com", "/compatible-mode/v1/chat/completions", null,
                        List.of()));

        ProviderRouteDefinitionLocator locator = new ProviderRouteDefinitionLocator(new AiGatewayProperties());
        List<RouteDefinition> once = locator.build(providers);
        List<RouteDefinition> twice = locator.build(providers);

        assertThat(twice).hasSameSizeAs(once);
        assertThat(twice).extracting(RouteDefinition::getId)
                .isEqualTo(once.stream().map(RouteDefinition::getId).toList());
    }

    private static RouteDefinition routeById(List<RouteDefinition> routes, String id) {
        return routes.stream().filter(definition -> id.equals(definition.getId())).findFirst().orElseThrow();
    }
}
