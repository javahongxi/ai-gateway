package org.hongxi.ai.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import org.hongxi.ai.gateway.config.AiGatewayProperties;
import org.hongxi.ai.gateway.config.UpstreamProperties;

/**
 * 大模型网关入口。路由与厂商凭证全部来自配置（本地 application.yml 加 Nacos 的
 * ai-gateway.yaml），代码里只承载配置表达不了的部分——见 {@code TenantAuthFilter} 与
 * {@code UpstreamIdleTimeoutCustomizer}。
 */
@SpringBootApplication
@EnableConfigurationProperties({ AiGatewayProperties.class, UpstreamProperties.class })
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
