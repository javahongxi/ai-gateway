package org.hongxi.ai.gateway;


import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.ResponseObservationFilter;
import org.hongxi.ai.gateway.resilience.ProviderHealthProbe;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R15：探活与自动恢复。
 *
 * <p>定时那一档在这里是关掉的（{@code ai-gateway.probe.enabled=false}），断言直接调
 * {@code probeAll()}——真实时间不参与断言，理由与限流用例那条一样：让调度器参与，同一个测试
 * 就会因为机器快慢给两种结果。开关本身另外有一条用例守着。
 *
 * <p>这里最要紧的一条断言不是"熔断收闸"，而是<b>探测不污染业务数据</b>：它走独立连接、绕开
 * 过滤器链，所以 {@code ai.gateway.requests} 的样本数与访问日志的行数都不该因为一次探测而变动。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=health-probe",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "management.server.port=0",
        // 定时那一档关掉，测试自己调 probeAll()
        "ai-gateway.probe.enabled=false",
        "ai-gateway.generated-routes=false",
        "ai-gateway.providers.probed.base-url=http://127.0.0.1:1",
        "ai-gateway.providers.probed.chat-path=/v1/chat/completions",
        "ai-gateway.providers.probed.key-ref=probed-secret",
        "ai-gateway.providers.probed.health-path=/models/probed",
        "ai-gateway.providers.unprobed.base-url=http://127.0.0.1:1",
        "ai-gateway.providers.unprobed.chat-path=/v1/chat/completions",
        "ai-gateway.providers.unprobed.key-ref=unprobed-secret"
})
class ProviderHealthProbeTest {

    private static final StubUpstream upstream = StubUpstream.start();

    @Autowired
    private ProviderHealthProbe probe;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    @Autowired
    private MeterRegistry registry;

    private ListAppender<ILoggingEvent> accessLogs;

    @DynamicPropertySource
    static void stubAddresses(DynamicPropertyRegistry registry) {
        // 两个厂商都指向桩：探测要看的"活着/死了"是桩的场景，不是真厂商的抖动
        registry.add("ai-gateway.providers.probed.base-url", upstream::baseUrl);
        registry.add("ai-gateway.providers.unprobed.base-url", upstream::baseUrl);
    }

    @BeforeEach
    void startClean() {
        upstream.reset();
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(ResponseObservationFilter.ACCESS_LOGGER)).addAppender(accessLogs);
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
    }

    /** 探到 2xx：熔断收闸，且业务指标与访问日志一个字都没多。 */
    @Test
    void healthyProbeClosesTheBreakerWithoutTouchingBusinessMetrics() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("probed");
        breaker.transitionToOpenState();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        long businessSamplesBefore = businessSampleCount();
        int hitsBefore = upstream.hits("/models/probed");

        probe.probeAll();

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        assertThat(counter(ProviderHealthProbe.PROBE_METRIC, "healed", "probed")).isGreaterThanOrEqualTo(1D);
        assertThat(healthyGauge("probed")).isEqualTo(1D);
        assertThat(upstream.hits("/models/probed")).isEqualTo(hitsBefore + 1);
        assertThat(businessSampleCount())
                .as("探测绕开过滤器链，不该给业务延迟分位数添样本")
                .isEqualTo(businessSamplesBefore);
        assertThat(accessLogs.list).as("探测不该落访问日志").isEmpty();
    }

    /** 探到非 2xx：只记指标，状态留给熔断自己判——两个观察者抢一台状态机是事故来源。 */
    @Test
    void unhealthyProbeRecordsButDoesNotChangeState() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("probed");
        breaker.transitionToOpenState();
        upstream.scenario(StubUpstream.Scenario.FAIL_500);

        probe.probeAll();

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(counter(ProviderHealthProbe.PROBE_METRIC, "unhealthy", "probed")).isGreaterThanOrEqualTo(1D);
        assertThat(healthyGauge("probed")).isZero();
    }

    /** 没配 health-path 就跳过，并且一次请求都不发：不猜端点。 */
    @Test
    void providerWithoutHealthPathIsSkippedWithoutARequest() {
        circuitBreakers.circuitBreaker("unprobed").transitionToOpenState();
        int requestsBefore = upstream.requests();

        probe.probeAll();

        assertThat(counter(ProviderHealthProbe.PROBE_METRIC, "skipped", "unprobed")).isGreaterThanOrEqualTo(1D);
        assertThat(upstream.requests())
                .as("跳过的厂商不该产生任何外部调用")
                .isEqualTo(requestsBefore);
        assertThat(circuitBreakers.find("unprobed")).get()
                .extracting(CircuitBreaker::getState).isEqualTo(CircuitBreaker.State.OPEN);
    }

    /** 活着的厂商不敲：这一项是全部外部探测流量的来源，省着用。 */
    @Test
    void closedBreakersAreNotProbed() {
        circuitBreakers.circuitBreaker("probed");
        int hitsBefore = upstream.hits("/models/probed");

        probe.probeAll();

        assertThat(counter(ProviderHealthProbe.PROBE_METRIC, "already-closed", "probed")).isGreaterThanOrEqualTo(1D);
        assertThat(upstream.hits("/models/probed")).isEqualTo(hitsBefore);
    }

    /** 开关只管定时入口，不影响直接调用——否则测试里没法验证探测逻辑本身。 */
    @Test
    void enabledSwitchGatesOnlyTheScheduledEntry() {
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("probed");
        breaker.transitionToOpenState();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);

        probe.scheduledProbe();
        assertThat(breaker.getState())
                .as("enabled=false 时定时入口什么都不做")
                .isEqualTo(CircuitBreaker.State.OPEN);

        probe.probeAll();
        assertThat(breaker.getState())
                .as("直接调用仍然工作，测试与运维脚本靠的就是这个入口")
                .isEqualTo(CircuitBreaker.State.CLOSED);
    }

    private long businessSampleCount() {
        return registry.find(org.hongxi.ai.gateway.observability.RequestMetricsFilter.REQUEST_METRIC)
                .timers().stream().mapToLong(Timer::count).sum();
    }

    private double counter(String metric, String outcome, String provider) {
        Counter found = registry.find(metric).tag("outcome", outcome).tag("provider", provider).counter();
        return found == null ? 0D : found.count();
    }

    private double healthyGauge(String provider) {
        Gauge found = registry.find(ProviderHealthProbe.HEALTHY_GAUGE).tag("provider", provider).gauge();
        return found == null ? Double.NaN : found.value();
    }
}
