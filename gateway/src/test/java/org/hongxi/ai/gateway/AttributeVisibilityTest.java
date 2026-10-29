package org.hongxi.ai.gateway;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.server.ServerWebExchange;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 机制探针：内层过滤器往 exchange 属性里写东西，外层（访问日志所在那一层）看不看得见。
 *
 * <p>起因是一个真实缺陷：用量过滤器在 {@code doFinally} 里把采到的 Usage 放进属性表，日志那一层
 * （更外层）读出来却是 null——现象是"单跑绿、整包红"，而日志里 status 明明写着 200。把它做成测试，
 * 是因为这类结论只配被复现，不配被记住。
 *
 * <p>这里只断言可复现的现象，不解释成因（mutate() 是否复制了属性表、两个完成回调谁先跑，都没验到
 * 那一层）。取用的规矩是一条：<b>别指望内层在完成回调里写的属性被外层读到</b>——要传东西就在请求
 * 阶段传（外层先放一个可变盒子，内层往里装），或者干脆由外层自己算。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=attribute-visibility",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.generated-routes=false",
        "spring.cloud.gateway.server.webflux.routes[0].id=attr-probe",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=attr",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=attr"
})
@Import({AlwaysAllowRateLimiter.class, AttributeVisibilityTest.Filters.class})
class AttributeVisibilityTest {

    static final String COMPLETION_PHASE_KEY = "attrWrittenInDoFinally";

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @TestConfiguration
    static class Filters {

        @Bean
        AtomicReference<String> observedCompletionPhase() {
            return new AtomicReference<>();
        }

        @Bean
        InnerWriter innerWriter() {
            return new InnerWriter();
        }

        @Bean
        OuterReader outerReader(AtomicReference<String> observedCompletionPhase) {
            return new OuterReader(observedCompletionPhase);
        }
    }

    /** 内层过滤器：装饰响应（观测类过滤器都要做的事），然后在自己的完成回调里写一笔属性。 */
    static class InnerWriter implements GlobalFilter, Ordered {

        @Override
        public int getOrder() {
            return -20;
        }

        @Override
        public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
            ServerHttpResponse decorated = new ServerHttpResponseDecorator(exchange.getResponse());
            return chain.filter(exchange.mutate().response(decorated).build())
                    .doFinally(signal -> exchange.getAttributes().put(COMPLETION_PHASE_KEY, "written-by-inner"));
        }
    }

    /** 外层过滤器（访问日志所在那一层）：在自己的完成回调里读。 */
    static class OuterReader implements GlobalFilter, Ordered {

        private final AtomicReference<String> observedCompletionPhase;

        OuterReader(AtomicReference<String> observedCompletionPhase) {
            this.observedCompletionPhase = observedCompletionPhase;
        }

        @Override
        public int getOrder() {
            return Ordered.HIGHEST_PRECEDENCE + 1;
        }

        @Override
        public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
            return chain.filter(exchange).doFinally(signal -> observedCompletionPhase.set(
                    String.valueOf(exchange.getAttributes().get(COMPLETION_PHASE_KEY))));
        }
    }

    @LocalServerPort
    private int port;

    @Autowired
    private AtomicReference<String> observedCompletionPhase;

    @Test
    void completionPhaseAttributeWrittenByAnInnerFilterIsNotVisibleOutOfIt() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        GatewayCalls calls = new GatewayCalls("http://127.0.0.1:" + port, "{\"model\":\"m\",\"messages\":[]}");
        assertThat(calls.call("/v1/chat/completions", "Bearer sk-alpha").status())
                .as("探针自己得先通，不然测的是网络不是可见性")
                .isEqualTo(200);

        // 外层那一笔是在响应写完之后才落的，与客户端拿到最后一个字节没有 happens-before——
        // 所以这里也要等它落定（与访问日志同一件事，见 docs 决策 D11 第二条）。
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (observedCompletionPhase.get() == null && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        assertThat(observedCompletionPhase.get())
                .as("外层确实跑了读逻辑，但读到的是缺席：内层 doFinally 写的属性传不出来")
                .isEqualTo("null");
    }
}
