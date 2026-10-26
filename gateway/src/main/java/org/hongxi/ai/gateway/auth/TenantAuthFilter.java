package org.hongxi.ai.gateway.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.config.AiGatewayProperties;

/**
 * 鉴权与租户隔离，排在整条过滤器链最前面：一个没有身份的请求不应该碰到上游厂商，
 * 否则配额、密钥、计费三件事都失去意义。
 *
 * <p>只做两件事——把 key 换成租户身份、检查这个租户能不能访问本条路由对应的厂商。它不碰
 * 请求体与响应体（见 docs/requirements.md 决策 D1），所以流式首字延迟不受影响。
 *
 * <p>错误响应用 OpenAI 的错误信封而不是自定义结构：客户端本来就按这个形状解析，
 * 网关没必要在入口制造第二套约定。
 */
@Component
public class TenantAuthFilter implements GlobalFilter, Ordered {

    /** 鉴权成功后挂到 exchange 上的租户名，供后续过滤器与日志使用。 */
    public static final String TENANT_ATTRIBUTE = "aiGatewayTenant";

    /** 转发给上游的租户头，让厂商侧日志与后端服务能按租户归因。 */
    public static final String TENANT_HEADER = "X-Tenant-Id";

    /** 路由 metadata 里的厂商别名，与 {@code ai-gateway.tenants.*.providers} 对齐。 */
    static final String PROVIDER_METADATA_KEY = "provider";

    private final AiGatewayProperties properties;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public TenantAuthFilter(AiGatewayProperties properties) {
        this.properties = properties;
    }

    @Override
    public int getOrder() {
        // 让指标过滤器（HIGHEST_PRECEDENCE）排在前面：鉴权一旦短路，链就不再往下走，
        // 排在后面的过滤器连那次拒绝都看不到。仍然早于 RouteToRequestUrlFilter(10000)
        // 与 NettyWriteResponseFilter(-1)，所以请求绝不会在没身份时碰到厂商。
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String presented = bearerToken(exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION));
        if (!StringUtils.hasText(presented)) {
            return reject(exchange, HttpStatus.UNAUTHORIZED, "invalid_request_error", "invalid_api_key",
                    "Missing API key: send an Authorization: Bearer <gateway key> header.");
        }
        Map<String, AiGatewayProperties.Tenant> tenants =
                properties.getTenants() == null ? Map.of() : properties.getTenants();
        String tenantName = matchTenant(tenants, presented);
        if (tenantName == null) {
            return reject(exchange, HttpStatus.UNAUTHORIZED, "invalid_request_error", "invalid_api_key",
                    "The API key is not recognised or has been revoked.");
        }

        AiGatewayProperties.Tenant tenant = tenants.get(tenantName);
        String provider = providerOf(exchange);
        if (provider != null && !tenant.allows(provider)) {
            return reject(exchange, HttpStatus.FORBIDDEN, "permission_denied_error", "provider_not_allowed",
                    "Tenant '%s' is not allowed to access provider '%s'.".formatted(tenantName, provider));
        }

        // 属性写在原 exchange 上：mutate() 出来的新 exchange 共享同一份 attribute，
        // 但先写再 mutate 不依赖这个实现细节。
        exchange.getAttributes().put(TENANT_ATTRIBUTE, tenantName);
        // set 而非 add：客户端自己塞的 X-Tenant-Id 必须被覆盖，否则租户身份可被伪造
        ServerWebExchange authenticated = exchange.mutate()
                .request(builder -> builder.headers(headers -> headers.set(TENANT_HEADER, tenantName)))
                .build();
        return chain.filter(authenticated);
    }

    /** {@code Authorization: Bearer xxx} 里的 xxx；大小写不敏感，其它认证方案一律视为无 key。 */
    private static String bearerToken(String authorizationHeader) {
        if (!StringUtils.hasText(authorizationHeader)) {
            return null;
        }
        String prefix = "Bearer ";
        if (authorizationHeader.regionMatches(true, 0, prefix, 0, prefix.length())) {
            return authorizationHeader.substring(prefix.length()).trim();
        }
        return null;
    }

    /**
     * 定长比较，避免用响应时间逐字节猜 key。租户是几十这个量级，线性扫描不是瓶颈，
     * 换来的是配置刷新后不需要维护一份会失效的索引。
     */
    private static String matchTenant(Map<String, AiGatewayProperties.Tenant> tenants, String presented) {
        byte[] actual = presented.getBytes(StandardCharsets.UTF_8);
        for (Map.Entry<String, AiGatewayProperties.Tenant> entry : tenants.entrySet()) {
            String expected = entry.getValue().getApiKey();
            if (!StringUtils.hasText(expected)) {
                continue;
            }
            if (MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * 本条路由的厂商别名。路由在 handler mapping 阶段就挂到了 exchange 上，所以本过滤器
     * 即使排在最前也读得到；没有 metadata 时返回 null，表示这条路由不限厂商。
     */
    private static String providerOf(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        if (route == null || route.getMetadata() == null) {
            return null;
        }
        Object provider = route.getMetadata().get(PROVIDER_METADATA_KEY);
        return provider == null ? null : provider.toString();
    }

    private Mono<Void> reject(ServerWebExchange exchange, HttpStatus status, String type, String code,
            String message) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        // param 按 OpenAI 的形状留空位（null），Map.of 不收 null，所以用 LinkedHashMap
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("param", null);
        error.put("code", code);
        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(Map.of("error", error));
        }
        catch (JsonProcessingException impossible) {
            body = ("{\"error\":{\"message\":\"gateway rejected the request\",\"type\":\"" + type
                    + "\",\"param\":null,\"code\":\"" + code + "\"}}").getBytes(StandardCharsets.UTF_8);
        }
        return response.writeWith(Mono.just(response.bufferFactory().wrap(body)));
    }
}
