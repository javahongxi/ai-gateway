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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
 * R14 的端到端那一半：判定结果真的喂进了熔断状态机，所以"连续命中达到阈值后改道备用厂商"是行为
 * 而不是文档里的说法。
 *
 * <p>拓扑是两条手写路由：{@code flaky}（会回 200 + 错误体）和 {@code healthy}（它的备份）。
 * 关键在于<b>当前那一发不改</b>：响应已经开始下发就撤不回来（D6.3 同一条边界），被改道的是后面
 * 的请求。所以第三用例先看熔断打开，再确认厂商恢复之后仍被绕开——那才是熔断该有的行为。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=semantic-failure",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.generated-routes=false",
        // 只有声明了这个码的厂商才会因它命中 indicator 规则
        "ai-gateway.providers.flaky.base-url=http://127.0.0.1:65020",
        "ai-gateway.providers.flaky.chat-path=/upstream/flaky",
        "ai-gateway.providers.flaky.key-ref=flaky-secret",
        "ai-gateway.providers.flaky.failure-indicators[0]=Throttling.AllocationQuota",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=flaky",
        "ai-gateway.tenants.alpha.providers[1]=healthy",
        "spring.cloud.gateway.server.webflux.routes[0].id=flaky-chat",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=flaky",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].name=CircuitBreaker",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].args.name=flaky",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].args.fallbackUri="
                + "forward:/backup/healthy/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0].args.statusCodes[0]=INTERNAL_SERVER_ERROR",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1]=SetPath=/upstream/flaky",
        "spring.cloud.gateway.server.webflux.routes[1].id=backup-healthy",
        "spring.cloud.gateway.server.webflux.routes[1].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[1].predicates[0]=Path=/backup/healthy/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[1].metadata.provider=healthy",
        "spring.cloud.gateway.server.webflux.routes[1].filters[0]=SetPath=/upstream/healthy",
        // 熔断窗口保持默认口径；开闸由连续语义命中计数触发（阈值见上面的属性）。
    // 不自动进半开：要看的就是"打开之后不再敲这家"
        "resilience4j.circuitbreaker.configs.default.sliding-window-type=count_based",
        "resilience4j.circuitbreaker.configs.default.sliding-window-size=2",
        "resilience4j.circuitbreaker.configs.default.minimum-number-of-calls=2",
        "resilience4j.circuitbreaker.configs.default.failure-rate-threshold=50",
        "resilience4j.circuitbreaker.configs.default.wait-duration-in-open-state=60s",
        "resilience4j.timelimiter.configs.default.timeout-duration=120s",
        // 测试里把阈值压到 2，免得为验证开闸先打三发
        "ai-gateway.semantic-failure-threshold=2"
})
@Import(AlwaysAllowRateLimiter.class)
class SemanticFailureCircuitTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String BODY = "{\"model\":\"anything\",\"messages\":[]}";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    private GatewayCalls calls;

    private ListAppender<ILoggingEvent> accessLogs;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @BeforeEach
    void startClean() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, BODY);
        upstream.reset();
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(ResponseObservationFilter.ACCESS_LOGGER)).addAppender(accessLogs);
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
    }

    /** 命中之后：客户端拿到的正文一个字都不改，日志与计数各记一笔。 */
    @Test
    void semanticFailureIsPassedThroughButRecorded() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_ERROR_BODY);
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(before);

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(outcome.body())
                .as("响应已经开始下发就撤不回来（D6.3），网关不替厂商把错误体改成别的形状")
                .isEqualTo(StubUpstream.ERROR_BODY);
        JsonNode line = lastLine();
        assertThat(line.path("semanticFailure").path("kind").asText()).isEqualTo("structural");
        assertThat(line.path("semanticFailure").path("detail").asText()).isEqualTo("rate_limit_exceeded");
        assertThat(semanticCount("structural")).isGreaterThanOrEqualTo(1D);
    }

    @Test
    void successfulResponseCarriesNoSemanticField() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(before);

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(lastLine().has("semanticFailure")).isFalse();
        assertThat(lastLine().path("usage").path("completionTokens").asInt()).isZero();
    }

    /**
     * 连续命中把熔断打开，之后厂商恢复正常也仍然被绕开——这才是"计入熔断"的意义：治的是后面的
     * 请求，不是当前这一发。
     */
    @Test
    void repeatedHitsOpenTheBreakerAndRouteAroundTheProvider() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_ERROR_BODY);
        // 两发都要等到那一行日志落下再断言：开闸动作与日志同一处、在完成回调里做，
        // 不等就是拿"客户端已收到响应"去赌"网关已处理完"——整包跑时这个赌会输（实测红过一次）
        int beforeFirst = accessLogs.list.size();
        calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(beforeFirst);
        int beforeSecond = accessLogs.list.size();
        calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(beforeSecond);

        assertThat(circuitBreakers.find("flaky"))
                .as("连续两次语义命中就该开闸")
                .get()
                .extracting(CircuitBreaker::getState)
                .isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(upstream.hits("/upstream/flaky")).isEqualTo(2);

        // 厂商恢复了：熔断还开着，这一发不该再敲它，而要由备份接走
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        int beforeThird = accessLogs.list.size();
        Outcome served = calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(beforeThird);

        assertThat(served.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(served.body()).contains("chatcmpl-stub");
        assertThat(upstream.hits("/upstream/flaky")).as("打开之后不该再敲那家").isEqualTo(2);
        assertThat(upstream.hits("/upstream/healthy")).isEqualTo(1);
    }

    /**
     * 那一行是在外层过滤器的 doFinally 里落的，客户端拿到最后一个字节与它之间没有
     * happens-before——整包跑时这里就会偶发读到空（单跑绿）。用法则是先记条数再等它变多。
     */
    private void awaitNewLine(int before) {
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
        assertThat(accessLogs.list.size()).as("这次请求应该留下一行访问日志").isGreaterThan(before);
    }

    private double semanticCount(String kind) {
        Counter counter = registry.find(ResponseObservationFilter.SEMANTIC_FAILURE_METRIC)
                .tag("kind", kind).tag("provider", "flaky").counter();
        return counter == null ? 0D : counter.count();
    }

    private JsonNode lastLine() {
        List<String> all = accessLogs.list.stream().map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList());
        assertThat(all).as("访问日志应该有一行").isNotEmpty();
        String line = all.get(all.size() - 1);
        try {
            return MAPPER.readTree(line);
        }
        catch (Exception parsing) {
            throw new AssertionError("访问日志不是合法 JSON: " + line, parsing);
        }
    }
}
