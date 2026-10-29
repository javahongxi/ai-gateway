package org.hongxi.ai.gateway.routing;

import java.util.ArrayList;
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
 * 模型组（R20）展开成改道链的结构断言：谁是对外入口、谁只在内部、每一跳带自己的真名、最后一跳不再
 * 挂熔断。这些一旦写错就是"看起来配了、其实不会改道"，所以必须能在不起上下文时就被钉住。
 */
class ModelGroupRoutesTest {

    private static AiGatewayProperties.Provider provider(String baseUrl, String chatPath) {
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setBaseUrl(baseUrl);
        declared.setChatPath(chatPath);
        declared.setKeyRef("sk-" + Math.abs(baseUrl.hashCode()));
        return declared;
    }

    private static AiGatewayProperties.ModelGroupEntry hop(String provider, String model) {
        AiGatewayProperties.ModelGroupEntry entry = new AiGatewayProperties.ModelGroupEntry();
        entry.setProvider(provider);
        entry.setModel(model);
        return entry;
    }

    private static Map<String, List<AiGatewayProperties.ModelGroupEntry>> group(String name,
            AiGatewayProperties.ModelGroupEntry... members) {
        Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups = new LinkedHashMap<>();
        groups.put(name, new ArrayList<>(List.of(members)));
        return groups;
    }

    private static Map<String, AiGatewayProperties.Provider> providers() {
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("primary", provider("https://primary.invalid", "/v1/chat/completions"));
        providers.put("spare", provider("https://spare.invalid", "/compatible-mode/v1/chat/completions"));
        return providers;
    }

    private static List<RouteDefinition> routes(Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups) {
        return new ProviderRouteDefinitionLocator(new AiGatewayProperties()).build(providers(), groups);
    }

    private static RouteDefinition byId(List<RouteDefinition> routes, String id) {
        return routes.stream().filter(r -> id.equals(r.getId())).findFirst().orElseThrow();
    }

    @Test
    void groupRoutesComeFirstAndCarryTheirOwnModelName() {
        List<RouteDefinition> routes = routes(group("chat-default", hop("primary", "primary-real"),
                hop("spare", "spare-real")));

        assertThat(routes).extracting(RouteDefinition::getId)
                .as("组路由在厂商路由之前（它认领的是确定名字）")
                .startsWith("group-chat-default-0-primary", "group-chat-default-1-spare");
        assertThat(routes.get(0).getOrder()).isEqualTo(ProviderRouteDefinitionLocator.MODEL_GROUP_ROUTE_ORDER);

        @SuppressWarnings("unchecked")
        Map<String, String> mapping = (Map<String, String>) routes.get(0).getMetadata()
                .get(ModelMappingGatewayFilterFactory.MODEL_MAPPING_METADATA_KEY);
        assertThat(mapping).containsEntry("chat-default", "primary-real");
        assertThat(bById(routes, "group-chat-default-1-spare").getMetadata())
                .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
                .extractingByKey(ModelMappingGatewayFilterFactory.MODEL_MAPPING_METADATA_KEY)
                .asString().contains("spare-real");
    }

    private static RouteDefinition bById(List<RouteDefinition> routes, String id) {
        return byId(routes, id);
    }

    @Test
    void onlyTheFirstHopIsThePublicEntry() {
        List<RouteDefinition> routes = routes(group("chat-default", hop("primary", "primary-real"),
                hop("spare", "spare-real")));

        assertThat(paths(byId(routes, "group-chat-default-0-primary")))
                .containsExactly("Path=/v1/chat/completions", "Model=chat-default");
        assertThat(paths(byId(routes, "group-chat-default-1-spare")))
                .as("非首跳只被上一跳的改道命中")
                .containsExactly("Path=/internal/chat-default/1/v1/chat/completions", "Model=chat-default");
    }

    /** 最后一跳不再挂熔断：它后面已经没有人了。 */
    @Test
    void lastHopHasNoBreakerAndOthersPointAtTheNextOne() {
        List<RouteDefinition> routes = routes(group("chat-default", hop("primary", "primary-real"),
                hop("spare", "spare-real")));

        assertThat(filterNames(byId(routes, "group-chat-default-0-primary")))
                .containsExactly("CircuitBreaker", "ProviderKey", "ModelMapping", "SetPath");
        assertThat(byId(routes, "group-chat-default-0-primary").getFilters().get(0).getArgs())
                .containsEntry("name", "primary")
                .containsEntry("fallbackUri", "forward:/internal/chat-default/1/v1/chat/completions");
        assertThat(filterNames(byId(routes, "group-chat-default-1-spare")))
                .containsExactly("ProviderKey", "ModelMapping", "SetPath");
    }

    @Test
    void brokenMemberIsDroppedNotTheWholeChain() {
        List<RouteDefinition> routes = routes(group("chat-default", hop("ghost", "ghost-real"),
                hop("primary", "primary-real"), hop("spare", "spare-real")));

        assertThat(routes).extracting(RouteDefinition::getId)
                .as("没声明的厂商只丢这一跳，链还在")
                .contains("group-chat-default-0-primary", "group-chat-default-1-spare")
                .doesNotContain("group-chat-default-0-ghost");
    }

    @Test
    void singleMemberGroupIsJustAPlainRoute() {
        List<RouteDefinition> routes = routes(group("only", hop("primary", "primary-real")));

        RouteDefinition only = byId(routes, "group-only-0-primary");
        assertThat(filterNames(only)).containsExactly("ProviderKey", "ModelMapping", "SetPath");
        assertThat(only.getPredicates()).extracting(PredicateDefinition::getName)
                .containsExactly("Path", "Model");
    }

    private static List<String> paths(RouteDefinition definition) {
        List<String> out = new ArrayList<>();
        for (PredicateDefinition predicate : definition.getPredicates()) {
            out.add(predicate.getName() + "=" + predicate.getArgs().values().iterator().next());
        }
        return out;
    }

    private static List<String> filterNames(RouteDefinition definition) {
        return definition.getFilters().stream().map(FilterDefinition::getName).toList();
    }
}
