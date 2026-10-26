package org.hongxi.ai.gateway;

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.FirstTokenMetricsFilter;
import org.hongxi.ai.gateway.observability.RequestMetricsFilter;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R9 剩下的两项：traceId 贯穿，以及首字延迟（TTFT）单独计量。
 *
 * <p>traceId 的验收点是「它真的到了厂商」而不是「日志里有」——传播头是排障时唯一能被
 * 厂商侧日志对上的东西，所以断言打在桩收到的 {@code traceparent} 上。
 *
 * <p>TTFT 的验收点是「它比整条请求短」：流式场景下首块 80ms、整条 560ms，两个数混成一个
 * 就等于没有测。
 */
@Import({AlwaysAllowRateLimiter.class, ObservabilityTest.MdcProbe.class})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // 每个测试类一句自己的标记：Spring 的上下文缓存键只看合并后的配置，
        // 不看 @DynamicPropertySource 贡献的值——两个类的光看注解会算同一个上下文，
        // 路由表就被前一个类复用了（实测整类跑时指标断言会飘）。
        "ai-gateway.test-context=observability",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "management.tracing.sampling.probability=1.0",
        "management.tracing.export.enabled=true",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=stub"
})
class ObservabilityTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry registry;

    private GatewayCalls calls;

    /** 抓一份请求执行期间的 MDC，日志格式读的就是这两个键。 */
    static final java.util.List<String> MDC_SNAPSHOT = new java.util.concurrent.CopyOnWriteArrayList<>();

    @org.springframework.boot.test.context.TestConfiguration(proxyBeanMethods = false)
    static class MdcProbe {

        @org.springframework.context.annotation.Bean
        org.springframework.cloud.gateway.filter.GlobalFilter mdcProbe(
                io.micrometer.tracing.Tracer tracer, io.micrometer.observation.ObservationRegistry observationRegistry) {
            return (exchange, chain) -> {
                MDC_SNAPSHOT.add(org.slf4j.MDC.get("traceId") + "/" + org.slf4j.MDC.get("spanId")
                        + "/tracer=" + (tracer == null ? "nobean" : String.valueOf(tracer.currentTraceContext().context()))
                        + "/obs=" + observationRegistry.getCurrentObservation());
                return chain.filter(exchange);
            };
        }
    }

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        String route = "spring.cloud.gateway.server.webflux.routes[0]";
        registry.add(route + ".id", () -> "stub-obs");
        registry.add(route + ".uri", upstream::baseUrl);
        registry.add(route + ".predicates[0]", () -> "Path=" + CHAT_PATH);
        registry.add(route + ".metadata.provider", () -> "stub");
        registry.add(route + ".filters[0]", () -> "SetPath=/upstream/chat");
    }

    @BeforeEach
    void connect() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
    }

    @Test
    void traceIdReachesTheProviderAsATraceparentHeader() {
        assertThat(calls.call(CHAT_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.OK.value());

        String traceparent = upstream.traceparent();
        assertThat(traceparent)
                .as("SCG 的客户端观测会把 W3C 头带给厂商，厂商侧日志才能与网关侧对上")
                .isNotNull()
                .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}");
    }

    @Test
    void traceIdIsNotInMdcUnderTheOtelBridge() {
        // 已知缺口，钉在这里而不是假装没有：WebFlux + micrometer-tracing-bridge-otel 下，
        // 请求执行中 MDC 的 traceId/spanId 都是 null，micrometer 的 Tracer.currentTraceContext()
        // 也拿不到——reactive 没有线程绑定的 scope。日志格式里写 %X{traceId} 只会打出空括号。
        // 传播本身是好的（见 traceIdReachesTheProviderAsATraceparentHeader）。
        // 要修：加 opentelemetry-logback-mdc 那个 appender instrumentation，或者从 Reactor
        // Context 里取 span 自己写日志。修好之后把这条断言反过来写，别删。

        calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(MDC_SNAPSHOT).isNotEmpty();
        assertThat(MDC_SNAPSHOT.get(MDC_SNAPSHOT.size() - 1)).startsWith("null/null/");
    }

    @Test
    void firstTokenIsRecordedApartFromTheWholeStream() {
        upstream.scenario(StubUpstream.Scenario.SSE_STEADY);
        // 桩每 80ms 一块共 7 块：首块约 80ms，整条约 560ms

        long ttftBefore = countOf(FirstTokenMetricsFilter.TTFT_METRIC);
        long totalBefore = countOf(RequestMetricsFilter.REQUEST_METRIC);
        double ttftNanosBefore = nanosOf(FirstTokenMetricsFilter.TTFT_METRIC);
        double totalNanosBefore = nanosOf(RequestMetricsFilter.REQUEST_METRIC);

        Outcome outcome = calls.call(CHAT_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        awaitCount(FirstTokenMetricsFilter.TTFT_METRIC, ttftBefore + 1);
        // 同一个上下文里多个用例共用 registry，所以断言增量而不是绝对值
        assertThat(countOf(FirstTokenMetricsFilter.TTFT_METRIC) - ttftBefore)
                .as("这一次请求记了一个首字样本").isEqualTo(1);
        assertThat(countOf(RequestMetricsFilter.REQUEST_METRIC) - totalBefore)
                .as("这一次请求记了一个总耗时样本").isEqualTo(1);
        assertThat(nanosOf(FirstTokenMetricsFilter.TTFT_METRIC) - ttftNanosBefore)
                .as("首字延迟必须明显短于整条请求，否则这两个指标是同一个数")
                .isLessThan(nanosOf(RequestMetricsFilter.REQUEST_METRIC) - totalNanosBefore);
        Timer ttft = registry.find(FirstTokenMetricsFilter.TTFT_METRIC).tag("route", "stub-obs").timer();
        assertThat(ttft.max(TimeUnit.MICROSECONDS))
                .as("首块在 80ms 附近，不该等到整条流结束")
                .isBetween(10_000d, 400_000d);
    }

    @Test
    void firstTokenCarriesTenantProviderAndRouteTags() {
        calls.call(CHAT_PATH, "Bearer sk-alpha");

        Timer ttft = registry.find(FirstTokenMetricsFilter.TTFT_METRIC).tag("route", "stub-obs").timer();
        assertThat(ttft).as("带 route tag 的那个 Timer 才是被记录的那个").isNotNull();
        assertThat(ttft.getId().getTag("tenant")).isEqualTo("alpha");
        assertThat(ttft.getId().getTag("provider")).isEqualTo("stub");
    }

    @Test
    void aRejectedRequestContributesNoFirstTokenSample() {
        long before = countOf(FirstTokenMetricsFilter.TTFT_METRIC);

        assertThat(calls.call(CHAT_PATH, null).status()).isEqualTo(HttpStatus.UNAUTHORIZED.value());

        assertThat(countOf(FirstTokenMetricsFilter.TTFT_METRIC))
                .as("没有响应字节就没有首字延迟，硬记一个 0 会把分位数拖歪")
                .isEqualTo(before);
    }

    /**
     * 同名指标的所有 Timer 计数之和。不能图省事用 {@code find(name).timer()}：那样在指标还不存在时
     * 得自己建一个无 tag 的 Timer，而真正被记录的是带 tag 的那个，两边对不上就永远读不到样本。
     */
    private long countOf(String name) {
        return registry.find(name).timers().stream().mapToLong(Timer::count).sum();
    }

    /** 指标在 doFinally 里记，客户端拿到响应时不一定已经落表，所以轮询而不是直读。 */
    private void awaitCount(String name, long atLeast) {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline && countOf(name) < atLeast) {
            try {
                Thread.sleep(20);
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private double nanosOf(String name) {
        return registry.find(name).timers().stream().mapToDouble(t -> t.totalTime(TimeUnit.NANOSECONDS)).sum();
    }
}
