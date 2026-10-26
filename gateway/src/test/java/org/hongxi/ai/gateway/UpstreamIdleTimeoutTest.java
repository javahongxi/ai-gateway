package org.hongxi.ai.gateway;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.LiveCall;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R11：上游开始回话之后卡住，空闲看门狗要在预算内断开。
 *
 * <p>与 {@code GatewayChatFlowTest} 分开成两个上下文是必须的：那边要把看门狗关掉，才能证明
 * {@code response-timeout} 管不到流中途；这边打开看门狗，证明补上之后确实管得到。同一个 Spring
 * 上下文里两个断言互相矛盾。
 *
 * <p>路由的 {@code response-timeout} 故意给到 5 秒，远大于看门狗的 400ms——这样「流被切断」只能
 * 来自看门狗，不会是被响应头超时顺手干的。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        // default-filters 里挂的是真限流（要 Redis、会扣配额），这些用例不测它
        // 主 yml 里的 optional:nacos import 仍会被处理（spring.config.import= 清空不了
        // 那份文档自带的 import），本机 Nacos 上没有 ai-gateway.yaml 时 SCA 会打一条 NPE
        // 堆栈。功能不受 optional: 影响，测试里把这条噪音压掉
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=400ms",
        // 三个测试类各自一个上下文，固定运维端口会互相抢；测试里随机化
        "management.server.port=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub"
})
@Import(AlwaysAllowRateLimiter.class)
class UpstreamIdleTimeoutTest {

    private static final String ROUTE = "spring.cloud.gateway.server.webflux.routes[0]";

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add(ROUTE + ".id", () -> "stub-idle");
        registry.add(ROUTE + ".uri", upstream::baseUrl);
        registry.add(ROUTE + ".predicates[0]", () -> "Path=" + CHAT_PATH);
        registry.add(ROUTE + ".metadata.provider", () -> "stub");
        registry.add(ROUTE + ".metadata.response-timeout", () -> "5000");
        registry.add(ROUTE + ".filters[0]", () -> "SetPath=/upstream/chat");
        registry.add(ROUTE + ".filters[1]", () -> "SetRequestHeader=Authorization,Bearer provider-secret");
    }

    @AfterAll
    static void shutDownUpstream() {
        upstream.close();
    }

    @BeforeEach
    void connectToGateway() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
    }

    @Test
    void stalledStreamIsAbortedByIdleTimeout() {
        upstream.scenario(StubUpstream.Scenario.SSE_THEN_SILENCE);
        // 吐一块之后不再说话：看门狗 400ms，响应头预算 5s

        LiveCall live = calls.start(CHAT_PATH, "Bearer sk-alpha");
        live.awaitQuietly(Duration.ofSeconds(3));

        assertThat(live.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(live.body()).as("第一块照常交付").contains("Hi");
        assertThat(live.terminated())
                .as("400ms 空闲预算内被切断，而不是等满 response-timeout 的 5 秒")
                .isTrue();
        // 用「结束时刻」而不是「最后一块到达时刻」：这个场景里只到了一块，
        // 到达时刻是 46ms，说明不了什么时候被切断
        assertThat(live.terminatedAfterMillis())
                .as("切断发生在空闲预算附近")
                .isBetween(350L, 2500L);
        // 实测：客户端看到的是 reactor.netty 的 PrematureCloseException「Connection prematurely
        // closed DURING response」——连接被掐断，不是干净的结束标记。看门狗保住的是网关自己
        // （连接与线程不再被无限占住）；要不要给客户端补一条可读的错误事件，见文档 R11。
        assertThat(live.failure()).as("以异常收尾，而不是正常完成").isNotNull();
        System.out.printf("idle-abort: %s%n", live.failure());
    }

    @Test
    void steadyStreamIsNotAffectedByIdleTimeout() {
        upstream.scenario(StubUpstream.Scenario.SSE_STEADY);
        // 每 80ms 一块，总时长 ~560ms 超过 400ms 的空闲预算，但相邻间隔没超——不该被切断

        GatewayCalls.Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(GatewayCalls.reassembleDeltas(outcome.body())).isEqualTo(StubUpstream.STEADY_BODY);
        assertThat(outcome.elapsed()).as("完整收完，用的时间比空闲预算长").isGreaterThan(400L);
    }
}
