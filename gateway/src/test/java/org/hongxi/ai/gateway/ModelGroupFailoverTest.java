package org.hongxi.ai.gateway;

import java.util.List;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.ResponseObservationFilter;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R20 端到端：一个虚拟模型名后面挂两家，按顺序改道，而且<b>每一跳用自己的真实模型名</b>。
 *
 * <p>这一组真正要证的是"改道之后名字也跟着换"。D6.4 那条真机失败（{@code qwen3.7-plus} 改道到
 * DeepSeek，被回一句"我们只认 deepseek-flash"）就是它；现在的机制是熔断在改道时用它自己拿到的
 * 那个请求重新分发，所以体里还是客户端传的虚拟名，第二跳的映射再把它换成第二跳的真名。
 *
 * <p>另一头也要守住：没人认领的名字仍然是 404，不是"随便塞给一家"。组机制最怕的就是把一个不认识的
 * 名字悄悄送去兜底厂商。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=model-group",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        // 三家的专名都指向一个永远不会用的名字：避免它们生成兜底路由，
        // 那样"没人认领该 404"就测不准了（生成器会先于组之外的路由抢走请求）
        "ai-gateway.providers.dead.chat-path=/upstream/dead",
        "ai-gateway.providers.dead.models[0]=zzz-dead-unused*",
        "ai-gateway.providers.dead.key-ref=dead-secret",
        "ai-gateway.providers.dead.connect-timeout=200",
        "ai-gateway.providers.dead.response-timeout=5000",
        "ai-gateway.providers.first.chat-path=/upstream/first",
        "ai-gateway.providers.first.models[0]=zzz-first-unused*",
        "ai-gateway.providers.first.key-ref=first-secret",
        "ai-gateway.providers.second.chat-path=/upstream/second",
        "ai-gateway.providers.second.models[0]=zzz-second-unused*",
        "ai-gateway.providers.second.key-ref=second-secret",
        // 生产 yml 里那两家也别当兜底
        "ai-gateway.providers.dashscope.models[0]=zzz-dashscope-unused*",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=dead",
        "ai-gateway.tenants.alpha.providers[1]=first",
        "ai-gateway.tenants.alpha.providers[2]=second",
        "ai-gateway.model-groups.chat-default[0].provider=dead",
        "ai-gateway.model-groups.chat-default[0].model=dead-real",
        "ai-gateway.model-groups.chat-default[1].provider=second",
        "ai-gateway.model-groups.chat-default[1].model=second-real",
        "ai-gateway.model-groups.healthy[0].provider=first",
        "ai-gateway.model-groups.healthy[0].model=first-real",
        "ai-gateway.model-groups.healthy[1].provider=second",
        "ai-gateway.model-groups.healthy[1].model=second-real",
        "resilience4j.circuitbreaker.configs.default.sliding-window-size=2",
        "resilience4j.circuitbreaker.configs.default.minimum-number-of-calls=2",
        "resilience4j.circuitbreaker.configs.default.failure-rate-threshold=50",
        "resilience4j.circuitbreaker.configs.default.wait-duration-in-open-state=60s",
        "resilience4j.timelimiter.configs.default.timeout-duration=120s"
})
@Import(AlwaysAllowRateLimiter.class)
class ModelGroupFailoverTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.providers.dead.base-url", () -> "http://127.0.0.1:65000");
        registry.add("ai-gateway.providers.first.base-url", upstream::baseUrl);
        registry.add("ai-gateway.providers.second.base-url", upstream::baseUrl);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    private GatewayCalls calls;

    private ListAppender<ILoggingEvent> accessLogs;

    @BeforeEach
    void startClean() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, null);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(ResponseObservationFilter.ACCESS_LOGGER)).addAppender(accessLogs);
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
    }

    private Outcome chat(String model) {
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha",
                "{\"model\":\"" + model + "\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}");
        awaitLine(before);
        return outcome;
    }

    /** 主跳连不上时，客户端拿到下一跳的成功响应，而且**体里是下一跳自己的真名**。 */
    @Test
    void failoverUsesTheNextHopsOwnModelName() {
        Outcome outcome = chat("chat-default");

        assertThat(outcome.status())
                .as("首跳连不上，应由第二跳接走（响应体：" + outcome.body() + "）")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/second")).isEqualTo(1);
        assertThat(upstream.body())
                .as("改道后不能再顶着第一跳的真名，也不能还是客户端那个虚拟名")
                .contains("\"model\":\"second-real\"")
                .doesNotContain("dead-real").doesNotContain("chat-default");
        assertThat(lastLine().path("requestedModel").asText()).isEqualTo("chat-default");
        assertThat(lastLine().path("upstreamModel").asText()).isEqualTo("second-real");
        assertThat(lastLine().path("route").asText()).contains("group-chat-default-1-second");
    }

    /** 主跳活着时用主跳的名字——组不是"永远走最后一家"。 */
    @Test
    void healthyChainUsesTheFirstHopAndItsName() {
        Outcome outcome = chat("healthy");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/first")).isEqualTo(1);
        assertThat(upstream.hits("/upstream/second")).isZero();
        assertThat(upstream.body()).contains("\"model\":\"first-real\"");
        assertThat(lastLine().path("upstreamModel").asText()).isEqualTo("first-real");
    }

    /** 首跳被熔断判死之后，后续请求不再敲它：换档治的是后面的请求，不是当前这一发。 */
    @Test
    void openBreakerSkipsTheDeadHopFromTheStart() {
        circuitBreakers.circuitBreaker("dead").transitionToOpenState();

        Outcome outcome = chat("chat-default");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(upstream.hits("/upstream/second")).isEqualTo(1);
        assertThat(lastLine().path("route").asText())
                .as("熔断打开后直接由第二跳接，不必再试一次死门")
                .contains("group-chat-default-1-second");
    }

    /** 虚拟名不认领别的厂商专名：组外的模型名照旧走厂商自己的路由或 404。 */
    @Test
    void unclaimedModelIsStillNotFound() {
        Outcome outcome = chat("nobody-claims-this");

        assertThat(outcome.status())
                .as("没人认领的名字不能被悄悄送去兜底厂商")
                .isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(upstream.requests()).isZero();
    }

    /** 内部跳不是对外入口：直接打它会被 Model 谓词拦住（客户端传的模型名不在组里）。 */
    @Test
    void internalHopIsNotAPublicEntry() {
        Outcome outcome = calls.call("/internal/chat-default/1/v1/chat/completions", "Bearer sk-alpha",
                "{\"model\":\"nobody\",\"messages\":[]}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(upstream.requests()).isZero();
    }

    /** 被鉴权挡掉的请求连组都不会进入判断。 */
    @Test
    void unauthorizedTenantNeverReachesTheChain() {
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-nobody",
                "{\"model\":\"chat-default\",\"messages\":[]}");

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(upstream.requests()).isZero();
    }

    private void awaitLine(int before) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (accessLogs.list.size() <= before && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private JsonNode lastLine() {
        List<String> all = accessLogs.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        assertThat(all).as("应该留下一行访问日志").isNotEmpty();
        String line = all.get(all.size() - 1);
        try {
            return MAPPER.readTree(line);
        }
        catch (Exception parsing) {
            throw new AssertionError("访问日志不是合法 JSON: " + line, parsing);
        }
    }
}
