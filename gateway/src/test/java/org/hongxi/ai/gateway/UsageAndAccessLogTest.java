package org.hongxi.ai.gateway;

import java.util.List;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import org.hongxi.ai.gateway.observability.AccessLogFilter;
import org.hongxi.ai.gateway.observability.UsageRecordingFilter;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R12 与 R13：用量采集与访问日志。
 *
 * <p>R12 的验收核心是<b>三种形状拿到同一个数</b>：非流式整段 JSON、流式把 usage 挂在
 * {@code finish_reason} 那个块上（DeepSeek）、流式在 {@code [DONE]} 之前多给一个 {@code choices:[]}
 * 的块（DashScope）。所以三条用例分别切一个场景，断言同一组增量。
 *
 * <p>还有一条与"不说谎"有关：厂商没回 usage 时记 {@code absent}，不记 0；被鉴权挡掉的请求则两种
 * 都不记（那次调用根本没发生）。
 *
 * <p>计数是累加的，每条用例先取基线再断言增量——不然同一个上下文里的第二条用例就会把第一条的数
 * 也算进去，测出来"每次都翻倍"。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=usage-access",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        // 本类的路由是自己注入的，关掉由 providers 生成那一档（见 D10）
        "ai-gateway.generated-routes=false",
        // 厂商声明仍然要用：ModelMapping 的映射表读的就是这里，而它和"生成路由"是两件事
        "ai-gateway.providers.usage.base-url=http://127.0.0.1:65010",
        "ai-gateway.providers.usage.chat-path=/upstream/usage",
        "ai-gateway.providers.usage.key-ref=usage-secret",
        "ai-gateway.providers.usage.model-mapping.chat-default=real-usage-model",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=usage",
        "spring.cloud.gateway.server.webflux.routes[0].id=usage-stub",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=usage",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0]=ModelMapping=usage",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1]=SetPath=/upstream/usage"
})
@Import(AlwaysAllowRateLimiter.class)
class UsageAndAccessLogTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String BODY = "{\"model\":\"chat-default\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry registry;

    private GatewayCalls calls;

    private ListAppender<ILoggingEvent> accessLogs;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @BeforeEach
    void attachLogCapture() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, BODY);
        upstream.reset();
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(AccessLogFilter.ACCESS_LOGGER)).addAppender(accessLogs);
    }

    private void callWith(StubUpstream.Scenario scenario) {
        upstream.scenario(scenario);
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");
        assertThat(outcome.status())
                .as("这一发本该成功，响应体是：" + outcome.body())
                .isEqualTo(HttpStatus.OK.value());
        awaitNewLine(before);
    }

    /**
     * 等这一行落到 appender 上。日志是在响应写完之后再落的（外层过滤器的 doFinally），客户端
     * 拿到最后一个字节与那一行之间没有 happens-before，直接读会读到空——实测就是三个用例随机红。
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

    /** 把 usage 摊成一条字符串比较，失败时一眼看出是哪个位置数错了。 */
    private String usageOfLastLine() {
        JsonNode usage = lastLine().path("usage");
        if (!usage.isObject()) {
            return "unknown";
        }
        return usage.path("promptTokens").asInt() + "/" + usage.path("completionTokens").asInt()
                + "/" + usage.path("cachedTokens").asInt() + "/" + usage.path("reasoningTokens").asInt();
    }

    /**
     * 三种形状、同一个数：这就是"不能假设倒数第二个块"那条实测结论的可执行版本。
     *
     * <p>断言看的是每行的日志而不是累加计数器。计数器跨上下文会累加，拿它做等式断言迟早被执行顺序
     * 与缓存的 Spring 上下文绊倒——这一版一开始就是那么写的，结果同一条断言在整包跑和单跑里给出
     * 两个结果。日志行是每请求一份，天然可证伪；计数器只单独断言注册出来了（见下面那条）。
     */
    @Test
    void allThreeUsageShapesLogTheSameNumbers() {
        callWith(StubUpstream.Scenario.ECHO_JSON_WITH_USAGE);
        assertThat(usageOfLastLine()).isEqualTo("11/7/4/3");
        callWith(StubUpstream.Scenario.SSE_USAGE_ON_FINISH_CHUNK);
        assertThat(usageOfLastLine()).isEqualTo("11/7/4/3");
        callWith(StubUpstream.Scenario.SSE_USAGE_IN_EXTRA_CHUNK);
        assertThat(usageOfLastLine()).isEqualTo("11/7/4/3");
    }

    /** token 计数按四个 type 分条注册，看板要能按类型堆叠；数值本身不在这里断言。 */
    @Test
    void tokenCountersArePublishedPerType() {
        callWith(StubUpstream.Scenario.ECHO_JSON_WITH_USAGE);

        for (String type : List.of("prompt", "completion", "cached", "reasoning")) {
            assertThat(registry.find(UsageRecordingFilter.USAGE_TOKENS_METRIC).tag("type", type).counter())
                    .as("%s 这一条 token 计数应该注册出来", type)
                    .isNotNull();
        }
    }

    /** 厂商没回 usage：日志里是字符串 unknown，不是四个 0——"没采到"不能长成"用了 0"。 */
    @Test
    void responseWithoutUsageLogsUnknownRatherThanZeros() {
        callWith(StubUpstream.Scenario.ECHO_JSON);

        JsonNode usage = lastLine().path("usage");
        assertThat(usage.isTextual()).as("unknown 应该是字符串").isTrue();
        assertThat(usage.asText()).isEqualTo("unknown");
    }

    /** 被鉴权挡掉的请求不产生 usage：那次调用压根没发生。日志那一行还是要有。 */
    @Test
    void rejectedRequestStillLogsOneLineButCarriesNoUsage() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON_WITH_USAGE);
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-wrong");
        awaitNewLine(before);

        assertThat(outcome.status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(upstream.requests()).isZero();
        assertThat(lines()).hasSize(1);
        assertThat(field(0, "status").asInt()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        assertThat(field(0, "tenant").asText()).isEqualTo("anonymous");
        assertThat(field(0, "usage").asText()).isEqualTo("unknown");
    }

    /** 日志行的字段：两个模型名分开、厂商请求 id 带上、客户端的 traceparent 原样记录。 */
    @Test
    void accessLineCarriesBothModelNamesAndTheUpstreamRequestId() {
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON_WITH_USAGE);
        String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        int before = accessLogs.list.size();
        Outcome outcome = calls.callWithHeader(CHAT_PATH, "Bearer sk-alpha", BODY, "traceparent", traceparent);
        awaitNewLine(before);

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(lines()).hasSize(1);
        assertThat(field(0, "tenant").asText()).isEqualTo("alpha");
        assertThat(field(0, "route").asText()).isEqualTo("usage-stub");
        assertThat(field(0, "provider").asText()).isEqualTo("usage");
        assertThat(field(0, "requestedModel").asText()).isEqualTo("chat-default");
        assertThat(field(0, "upstreamModel").asText())
                .as("D9 改写后的名字要单独看得见，否则改道与映射排障只能靠猜")
                .isEqualTo("real-usage-model");
        assertThat(field(0, "upstreamRequestId").asText()).isEqualTo(StubUpstream.REQUEST_ID);
        assertThat(field(0, "traceparent").asText()).isEqualTo(traceparent);
        assertThat(field(0, "usage").path("completionTokens").asInt()).isEqualTo(7);
        assertThat(field(0, "elapsedMs").asLong()).isGreaterThanOrEqualTo(0L);
    }

    /** 一次流式调用：首字延迟与整条耗时是两个数，日志里两个都在。 */
    @Test
    void streamingCallLogsBothFirstTokenLatencyAndTotalElapsed() {
        upstream.scenario(StubUpstream.Scenario.SSE_STEADY);
        int before = accessLogs.list.size();
        calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(before);

        assertThat(lines()).hasSize(1);
        JsonNode ttft = field(0, "ttftMs");
        assertThat(ttft.isMissingNode()).as("稳定吐字的流应该记到首字时刻").isFalse();
        assertThat(ttft.asLong())
                .as("TTFT 应该明显早于总耗时（7 块 × 80ms）")
                .isLessThan(field(0, "elapsedMs").asLong());
    }

    private JsonNode lastLine() {
        List<String> all = lines();
        assertThat(all).as("访问日志应该至少有一行").isNotEmpty();
        String line = all.get(all.size() - 1);
        try {
            return MAPPER.readTree(line);
        }
        catch (Exception parsing) {
            throw new AssertionError("访问日志不是合法 JSON: " + line, parsing);
        }
    }

    private List<String> lines() {
        return accessLogs.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    private JsonNode field(int index, String name) {
        try {
            return MAPPER.readTree(lines().get(index)).path(name);
        }
        catch (Exception parsing) {
            throw new AssertionError("访问日志不是合法 JSON: " + lines(), parsing);
        }
    }

}
