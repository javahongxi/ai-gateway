package org.hongxi.ai.gateway;

import java.util.List;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.ResponseObservationFilter;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R16 端到端：一家厂商两把 key 时，轮转、被限流那把单独冷却、以及"key 的原文绝不外流"。
 *
 * <p>最后一条是这一组里唯一会因为写错而伤人的：日志、指标、异常信息里出现完整上游凭证是最常见的
 * 泄漏方式之一，所以断言看的是实际发出去的 {@code Authorization} 与落到日志里的那串指纹。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=key-rotation",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.generated-routes=false",
        "spring.cloud.gateway.server.webflux.routes[0].id=pooled-chat",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=pooled",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0]=ProviderKey=pooled",
        "spring.cloud.gateway.server.webflux.routes[0].filters[1]=SetPath=/upstream/pooled",
        "ai-gateway.providers.pooled.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.pooled.chat-path=/upstream/pooled",
        "ai-gateway.providers.pooled.key-cooldown=30s",
        "ai-gateway.providers.pooled.keys[0]=sk-aaaa-1111",
        "ai-gateway.providers.pooled.keys[1]=sk-bbbb-2222",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=pooled"
})
@Import(AlwaysAllowRateLimiter.class)
class ProviderKeyRotationTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String BODY = "{\"model\":\"anything\",\"messages\":[]}";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @LocalServerPort
    private int port;

    private GatewayCalls calls;

    private ListAppender<ILoggingEvent> accessLogs;

    @BeforeEach
    void startClean() {
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        calls = new GatewayCalls("http://127.0.0.1:" + port, BODY);
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(ResponseObservationFilter.ACCESS_LOGGER)).addAppender(accessLogs);
    }

    /** 两把 key 轮着来：连续两发用的凭证必须不同，否则密钥池只是个装饰。 */
    @Test
    void consecutiveCallsUseDifferentKeys() {
        String first = call().status() == 200 ? upstream.authorization() : "call-failed";
        String second = call().status() == 200 ? upstream.authorization() : "call-failed";

        assertThat(first).startsWith("Bearer sk-");
        assertThat(second).startsWith("Bearer sk-");
        assertThat(second).as("同一个厂商的两把 key 应该轮换").isNotEqualTo(first);
    }

    /** 上游按 key 限流（429）时只有那一把进冷却，下一发应当换另一把。 */
    @Test
    void throttledKeyCoolsDownAndTheOtherOneKeepsServing() {
        upstream.scenario(StubUpstream.Scenario.TOO_MANY_REQUESTS);
        Outcome limited = call();
        String burnedKey = upstream.authorization();
        assertThat(limited.status()).as("厂商的 429 要原样透传").isEqualTo(429);

        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        call();

        assertThat(upstream.authorization())
                .as("刚被限流的那把 key 该冷却，另一把继续服务")
                .isNotEqualTo(burnedKey);
    }

    /** 日志里只有指纹；key 的原文一次都不该出现。 */
    @Test
    void logsCarryTheFingerprintAndNeverTheRawKey() {
        call();

        List<String> lines = lines();
        assertThat(lines).as("这一发该留下一行").isNotEmpty();
        String line = lines.get(lines.size() - 1);
        assertThat(json(line).path("upstreamKey").asText())
                .as("指纹是末四位，够定位是哪把，又不够复用")
                .isIn("****1111", "****2222");
        assertThat(line)
                .as("上游 key 的原文绝不进日志")
                .doesNotContain("sk-aaaa-1111").doesNotContain("sk-bbbb-2222");
    }

    private Outcome call() {
        int before = accessLogs.list.size();
        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");
        awaitNewLine(before);
        return outcome;
    }

    /** 日志行落在外层过滤器的完成回调里，与客户端完成没有 happens-before：等它变多。 */
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
    }

    private List<String> lines() {
        return accessLogs.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    private JsonNode json(String line) {
        try {
            return MAPPER.readTree(line);
        }
        catch (Exception parsing) {
            throw new AssertionError("访问日志不是合法 JSON: " + line, parsing);
        }
    }
}
