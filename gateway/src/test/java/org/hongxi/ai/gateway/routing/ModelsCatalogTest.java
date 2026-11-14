package org.hongxi.ai.gateway.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R22 清单构建的纯函数单测：不起上下文，直接把 providers/model-groups 声明喂给
 * {@link ModelsCatalog#build}，把「哪些名字进清单、归属记谁、怎么去重」变成断言。
 * 与生成器 {@code build} 同套路（决策 D18）。
 */
class ModelsCatalogTest {

    private static AiGatewayProperties.Provider providerWithMapping(Map<String, String> mapping) {
        AiGatewayProperties.Provider declared = new AiGatewayProperties.Provider();
        declared.setBaseUrl("https://example.com");
        declared.setChatPath("/v1/chat/completions");
        declared.setModelMapping(mapping);
        return declared;
    }

    private static List<String> ids(List<Map<String, Object>> models) {
        return models.stream().map(model -> (String) model.get("id")).toList();
    }

    @Test
    void aggregatesGroupNamesAndMappingKeysWithTheirOwners() {
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("deepseek", providerWithMapping(Map.of("deepseek-chat", "deepseek-v4")));
        providers.put("dashscope", providerWithMapping(Map.of("qwen-plus", "qwen3.7-plus")));
        Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups = new LinkedHashMap<>();
        groups.put("chat-default", List.of());

        List<Map<String, Object>> models = ModelsCatalog.build(providers, groups);

        assertThat(ids(models)).containsExactly("chat-default", "deepseek-chat", "qwen-plus");
        assertThat(models.get(0)).containsEntry("owned_by", ModelsCatalog.OWNED_BY_GATEWAY);
        assertThat(models.get(1)).containsEntry("owned_by", "deepseek");
        assertThat(models.get(2)).containsEntry("owned_by", "dashscope");
    }

    /** 每个模型都是 OpenAI 的形状：id / object=model / created / owned_by。 */
    @Test
    void everyEntryCarriesTheOpenAiModelShape() {
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("deepseek", providerWithMapping(Map.of("deepseek-chat", "deepseek-v4")));

        List<Map<String, Object>> models = ModelsCatalog.build(providers, Map.of());

        assertThat(models).hasSize(1);
        assertThat(models.get(0))
                .containsEntry("id", "deepseek-chat")
                .containsEntry("object", "model")
                .containsEntry("created", ModelsCatalog.CREATED)
                .containsEntry("owned_by", "deepseek");
    }

    /** 组名优先：同一个 id 既是组名又是某厂商映射键时，归属记网关而不是那家厂商。 */
    @Test
    void groupNameWinsOverProviderMappingKeyOnDuplicateId() {
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("deepseek", providerWithMapping(Map.of("chat-default", "deepseek-v4")));
        Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups = new LinkedHashMap<>();
        groups.put("chat-default", List.of());

        List<Map<String, Object>> models = ModelsCatalog.build(providers, groups);

        assertThat(ids(models)).containsExactly("chat-default");
        assertThat(models.get(0)).containsEntry("owned_by", ModelsCatalog.OWNED_BY_GATEWAY);
    }

    /** 厂商 models 里的通配是路由模式不是模型名，不进清单；只有 model-mapping 的键进。 */
    @Test
    void wildcardRoutingPatternsAreNotListedAsModels() {
        AiGatewayProperties.Provider declared = providerWithMapping(Map.of("deepseek-chat", "deepseek-v4"));
        declared.setModels(List.of("deepseek*"));
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("deepseek", declared);

        List<Map<String, Object>> models = ModelsCatalog.build(providers, Map.of());

        assertThat(ids(models)).containsExactly("deepseek-chat");
        assertThat(ids(models)).doesNotContain("deepseek*");
    }

    /** 没有映射表、没有组：清单为空而不是抛错。 */
    @Test
    void emptyDeclarationsProduceAnEmptyList() {
        assertThat(ModelsCatalog.build(Map.of(), Map.of())).isEmpty();
        assertThat(ModelsCatalog.build(null, null)).isEmpty();
        assertThat(ModelsCatalog.build(Map.of("x", providerWithMapping(Map.of())), Map.of())).isEmpty();
    }

    /** 空白名字不进清单：它不是一个能调的模型名。 */
    @Test
    void blankNamesAreSkipped() {
        Map<String, AiGatewayProperties.Provider> providers = new LinkedHashMap<>();
        providers.put("deepseek", providerWithMapping(Map.of("  ", "deepseek-v4")));
        Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups = new LinkedHashMap<>();
        groups.put("", List.of());

        assertThat(ModelsCatalog.build(providers, groups)).isEmpty();
    }
}