package org.hongxi.ai.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R16 的那句承诺：厂商维度的窗口按 {@code provider:key} 分桶，所以一把 key 用光配额不会把另一把
 * 一起判死。
 *
 * <p>窗口设成 1 次 / 600 秒，三发就够看：第一发用掉某把 key 的配额，第二发换到另一把仍然放行
 * ——这就是"按 key 分桶"；第三发必然撞回已经用掉一次的那把（两个桶、三发请求），于是被拒。
 * 如果窗口仍按厂商计，第二发就会被拒，这条用例当场红。
 *
 * <p>不钉死"第三发用的是哪一把"：轮换的相位不属于这条断言，钉死它只会让测试跟着实现细节抖。
 * 该钉的是"被拒那发用的 key 一定在已用过的两把里"，那才是配额按 key 走的证据。
 *
 * <p>与仓库里其他厂商限流用例一样，它需要本机 Redis（键形状 {@code ai-gateway:provider:*}）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=key-window",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.generated-routes=false",
        "spring.cloud.gateway.server.webflux.routes[0].id=windowed-chat",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=windowed",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0]=ProviderKey=windowed",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1]=SetPath=/upstream/windowed",
        "ai-gateway.providers.windowed.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.windowed.chat-path=/upstream/windowed",
        "ai-gateway.providers.windowed.keys[0]=sk-aaaa-1111",
        "ai-gateway.providers.windowed.keys[1]=sk-bbbb-2222",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=windowed",
        // 一个窗口只给一次：三发就能同时看到"换 key 放行"与"回到原 key 被拒"
        "ai-gateway.rate-limit.provider-requests-per-window=1",
        "ai-gateway.rate-limit.provider-window-seconds=600"
})
@Import(AlwaysAllowRateLimiter.class)
class ProviderKeyWindowTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @BeforeEach
    void startClean() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, "{\"model\":\"anything\",\"messages\":[]}");
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    @Test
    void oneKeysQuotaDoesNotExhaustTheOthers() {
        Outcome first = calls.call(CHAT_PATH, "Bearer sk-alpha");
        String firstKey = upstream.authorization();
        assertThat(first.status()).as("第一发该放行（响应体：" + first.body() + "）").isEqualTo(200);

        Outcome second = calls.call(CHAT_PATH, "Bearer sk-alpha");
        String secondKey = upstream.authorization();
        assertThat(secondKey).as("第二发换了 key").isNotEqualTo(firstKey);
        assertThat(second.status())
                .as("另一把 key 的配额是独立的，不该被前一把用光拖死")
                .isEqualTo(200);

        Outcome third = calls.call(CHAT_PATH, "Bearer sk-alpha");
        String thirdKey = upstream.authorization();
        assertThat(third.status())
                .as("三发两个桶，必有一把 key 吃到自己的第二个请求，按 key 分桶就该在这一发上拒")
                .isEqualTo(429);
        assertThat(thirdKey)
                .as("被拒的那发用的必须是前面已经用掉一次配额的 key，而不是第三把没见过的 key")
                .isIn(firstKey, secondKey);
    }
}
