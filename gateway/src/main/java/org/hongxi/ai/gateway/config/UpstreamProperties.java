package org.hongxi.ai.gateway.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 上游侧的传输保护。与路由 metadata 里的 {@code response-timeout} 不是一回事：那个管「等多久
 * 算厂商没回响应头」，这个管「已经开始回话之后，多久没新字节就断开」。
 *
 * @param idleTimeout 上游连接的空闲预算；为 0 或负数表示关掉看门狗
 */
@ConfigurationProperties(prefix = "ai-gateway.upstream")
public record UpstreamProperties(Duration idleTimeout) {

    public boolean idleTimeoutEnabled() {
        return idleTimeout != null && !idleTimeout.isZero() && !idleTimeout.isNegative();
    }
}
