package org.hongxi.ai.gateway.routing;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * R22：{@code /v1/models} 的清单产出端。它<b>不是对外入口</b>——挂在内部前缀 {@code /_gateway/}
 * 下，与 {@code /backup/}、{@code /internal/} 同类，只被生成器那条 {@code models-list} 路由
 * （{@code forward:/_gateway/models}）命中。
 *
 * <p><b>为什么不做成裸的对外控制器。</b>裸 {@code @RestController} 走 {@code RequestMappingHandlerMapping}
 * （order 0），会绕过整条网关过滤器链——{@code TenantAuthFilter} 是 {@code GlobalFilter}，只对路由
 * 请求生效。把清单直接挂出去等于把"有哪些厂商、哪些模型"匿名公开，而那是轻度敏感信息。走 forward
 * 路由则照常过鉴权、租户限流、访问日志与指标（决策 D18）。
 *
 * <p>每次请求<b>现读</b> {@link AiGatewayProperties}：Nacos 改完 providers/model-groups 即时反映，
 * 不依赖路由重建，也不在这里留一份会过期的缓存。清单怎么算见 {@link ModelsCatalog}。
 */
@RestController
public class ModelsController {

    /** 内部路径，与生成器 {@code models-list} 路由的 forward 目标对齐。 */
    static final String INTERNAL_PATH = "/_gateway/models";

    private final AiGatewayProperties properties;

    public ModelsController(AiGatewayProperties properties) {
        this.properties = properties;
    }

    @GetMapping(INTERNAL_PATH)
    public Map<String, Object> models() {
        // OpenAI SDK 的 models.list() 读的就是这个形状；LinkedHashMap 保住 object 在 data 前的顺序
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("object", "list");
        out.put("data", ModelsCatalog.build(properties.getProviders(), properties.getModelGroups()));
        return out;
    }
}