package org.hongxi.ai.gateway.routing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.util.StringUtils;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R22：网关对外可售的模型清单，从 {@code providers} 与 {@code model-groups} 声明聚合而来，
 * 不透传任何厂商。OpenAI SDK 的 {@code models.list()} 读的就是这个形状。
 *
 * <p><b>哪些名字进清单。</b>只有可枚举的对外名字：模型组的虚拟名（一个名字挂多家），以及各厂商
 * {@code model-mapping} 的<b>键</b>（客户端-facing 名字）。厂商 {@code models} 里的通配
 * （{@code deepseek*}）是<b>路由模式</b>不是具体模型名，进清单只会给客户端一个不能直接调的字符串，
 * 所以不进。按 {@code id} 去重，组名优先（它跨厂商，{@code owned_by} 记 {@value #OWNED_BY_GATEWAY}）。
 *
 * <p>纯函数，单测不起上下文——与 {@code ProviderRouteDefinitionLocator#build} 同套路。
 */
public final class ModelsCatalog {

    /** 模型组横跨多家厂商，归属记网关自己。 */
    public static final String OWNED_BY_GATEWAY = "ai-gateway";

    /** 清单里每个模型的 {@code created}。固定值，测试可断言；OpenAI 客户端不消费它。 */
    static final long CREATED = 0L;

    private ModelsCatalog() {
    }

    public static List<Map<String, Object>> build(Map<String, AiGatewayProperties.Provider> providers,
            Map<String, List<AiGatewayProperties.ModelGroupEntry>> groups) {
        Map<String, Map<String, Object>> byId = new LinkedHashMap<>();
        if (groups != null) {
            for (String virtualName : groups.keySet()) {
                if (StringUtils.hasText(virtualName)) {
                    byId.putIfAbsent(virtualName, model(virtualName, OWNED_BY_GATEWAY));
                }
            }
        }
        if (providers != null) {
            for (Map.Entry<String, AiGatewayProperties.Provider> entry : providers.entrySet()) {
                Map<String, String> mapping = entry.getValue().getModelMapping();
                if (mapping == null) {
                    continue;
                }
                for (String publicName : mapping.keySet()) {
                    if (StringUtils.hasText(publicName)) {
                        byId.putIfAbsent(publicName, model(publicName, entry.getKey()));
                    }
                }
            }
        }
        return new ArrayList<>(byId.values());
    }

    private static Map<String, Object> model(String id, String ownedBy) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("object", "model");
        out.put("created", CREATED);
        out.put("owned_by", ownedBy);
        return out;
    }
}