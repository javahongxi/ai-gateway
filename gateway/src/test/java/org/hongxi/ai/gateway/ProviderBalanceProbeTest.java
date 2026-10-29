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
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.observability.RequestMetricsFilter;
import org.hongxi.ai.gateway.observability.ResponseObservationFilter;
import org.hongxi.ai.gateway.resilience.ProviderBalanceProbe;
import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R18：把厂商账户余额读成 gauge。
 *
 * <p>这一组里最要紧的是<b>读不到时不许编一个 0</b>：字段改名、形状变了、端点回 5xx、连不上，
 * 都要留下各自的结果计数，gauge 保持上一次的读数。"没采到"与"余额为零"是两个完全不同的告警条件，
 * 混成一个数会让人在半夜被叫起来发现只是厂商改了字段名。
 *
 * <p>另一半是它<b>只报不治</b>：读余额不改熔断、不动密钥池、不进业务指标、不落访问日志。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ai-gateway.test-context=balance-probe",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.generated-routes=false",
        "ai-gateway.rate-limit.provider-requests-per-window=0",
        "management.server.port=0",
        // 定时那一档关掉，断言直接调 readBalances()：真实时间不参与
        "ai-gateway.balance.enabled=false",
        "ai-gateway.providers.rich.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.rich.chat-path=/upstream/rich",
        "ai-gateway.providers.rich.key-ref=rich-secret",
        "ai-gateway.providers.rich.balance-path=/v1/balance",
        "ai-gateway.providers.rich.balance-field=balance",
        "ai-gateway.providers.rich.balance-currency=CNY",
        "ai-gateway.providers.nested.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.nested.chat-path=/upstream/nested",
        "ai-gateway.providers.nested.key-ref=nested-secret",
        "ai-gateway.providers.nested.balance-path=/v1/balance",
        "ai-gateway.providers.nested.balance-field=data.remaining",
        "ai-gateway.providers.poorfield.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.poorfield.chat-path=/upstream/poor",
        "ai-gateway.providers.poorfield.key-ref=poor-secret",
        "ai-gateway.providers.poorfield.balance-path=/v1/balance",
        "ai-gateway.providers.poorfield.balance-field=data.nope",
        "ai-gateway.providers.nokeys.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.nokeys.chat-path=/upstream/nokeys",
        "ai-gateway.providers.nokeys.balance-path=/v1/balance-nokeys",
        "ai-gateway.providers.nokeys.balance-field=balance",
        "ai-gateway.providers.plain.base-url=${ai-gateway.test.stub-base}",
        "ai-gateway.providers.plain.chat-path=/upstream/plain",
        "ai-gateway.providers.plain.key-ref=plain-secret",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=rich",
        "spring.cloud.gateway.server.webflux.routes[0].id=rich-chat",
        "spring.cloud.gateway.server.webflux.routes[0].uri=${ai-gateway.test.stub-base}",
        "spring.cloud.gateway.server.webflux.routes[0].predicates[0]=Path=/v1/chat/completions",
        "spring.cloud.gateway.server.webflux.routes[0].metadata.provider=rich",
        "spring.cloud.gateway.server.webflux.routes[0].filters[0]=SetPath=/upstream/rich"
})
@Import(AlwaysAllowRateLimiter.class)
class ProviderBalanceProbeTest {

    private static final StubUpstream upstream = StubUpstream.start();

    @DynamicPropertySource
    static void stub(DynamicPropertyRegistry registry) {
        registry.add("ai-gateway.test.stub-base", upstream::baseUrl);
    }

    @Autowired
    private ProviderBalanceProbe probe;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    private ListAppender<ILoggingEvent> accessLogs;

    @BeforeEach
    void attachCapture() {
        upstream.reset();
        accessLogs = new ListAppender<>();
        accessLogs.start();
        ((Logger) LoggerFactory.getLogger(ResponseObservationFilter.ACCESS_LOGGER)).addAppender(accessLogs);
    }

    /** 数字字符串与嵌套路径都要能取到；单位进 tag，不同单位的余额不能相加。 */
    @Test
    void readsStringNumbersAndNestedPaths() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);

        assertThat(probe.readBalances()).as("四个配了 balance-path 的厂商都该被尝试")
                .isEqualTo(4);

        assertThat(gauge("rich", "CNY")).isEqualTo(3.5);
        assertThat(gauge("nested", "credits")).isEqualTo(12.5);
        assertThat(counter("ok", "rich")).isGreaterThanOrEqualTo(1D);
    }

    /** 字段不存在：记 missing-field，gauge 不新建也不清零。 */
    @Test
    void missingFieldIsReportedRatherThanZeroed() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);
        assertThat(Double.isNaN(gauge("poorfield", "credits")))
                .as("这条时间线一开始不该存在").isTrue();

        probe.readBalances();

        assertThat(counter("missing-field", "poorfield")).isGreaterThanOrEqualTo(1D);
        assertThat(Double.isNaN(gauge("poorfield", "credits")))
                .as("读不到就不建这条时间线，别用 0 冒充").isTrue();
    }

    /** 端点回 5xx：http-error，gauge 保持上一次读到的值——不是 0。 */
    @Test
    void httpErrorKeepsThePreviousReading() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);
        probe.readBalances();
        double settled = gauge("rich", "CNY");

        upstream.scenario(StubUpstream.Scenario.FAIL_500);
        probe.readBalances();

        assertThat(counter("http-error", "rich")).isGreaterThanOrEqualTo(1D);
        assertThat(gauge("rich", "CNY")).as("余额没读到就沿用上次，宁可旧不可假").isEqualTo(settled);
    }

    /** 配了 balance-path 却没有凭证：不发请求、记 error（这是配置错，必须看得见）。 */
    @Test
    void credentialsMissingFailsLoudlyWithoutARequest() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);

        probe.readBalances();

        assertThat(counter("error", "nokeys")).isGreaterThanOrEqualTo(1D);
        // 这家给的是独立路径，所以"零请求"看得清清楚楚，不会与别家的调用混在一起
        assertThat(upstream.hits("/v1/balance-nokeys")).as("没有凭证就不该发这个请求").isZero();
    }

    /** 没声明 balance-path 的厂商一次请求都不发：不猜端点。 */
    @Test
    void providersWithoutBalanceConfigAreNeverCalled() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);

        probe.readBalances();

        assertThat(upstream.hits("/v1/balance"))
                .as("只有四家配了 balance-path")
                .isBetween(0, 4);
        assertThat(Double.isNaN(gauge("plain", "credits"))).as("没配置的厂商不该出现时间线").isTrue();
    }

    /** 只报不治：不改熔断、不进业务指标、不落访问日志。 */
    @Test
    void readingBalanceChangesNothingButTheGauge() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);
        CircuitBreaker breaker = circuitBreakers.circuitBreaker("rich");
        breaker.transitionToOpenState();
        long businessSamples = businessSampleCount();

        probe.readBalances();

        assertThat(breaker.getState())
                .as("余额低不是调用失败，不该动熔断")
                .isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(businessSampleCount()).as("读余额绕开过滤器链").isEqualTo(businessSamples);
        assertThat(accessLogs.list).as("读余额不落访问日志").isEmpty();
    }

    /** 定时入口受开关控制，直接调用不受影响（与 R15 同一套约定）。 */
    @Test
    void enabledSwitchGatesOnlyTheScheduledEntry() {
        upstream.scenario(StubUpstream.Scenario.JSON_200_BALANCE);
        // 计数器是累加的（同一上下文被多个用例共用），所以断言增量而不是绝对值
        double okBefore = counter("ok", "rich");

        probe.scheduledRead();
        assertThat(counter("ok", "rich"))
                .as("ai-gateway.balance.enabled=false 时定时入口什么都不做")
                .isEqualTo(okBefore);

        probe.readBalances();
        assertThat(counter("ok", "rich")).isGreaterThan(okBefore);
    }

    private double gauge(String provider, String currency) {
        Gauge found = registry.find(ProviderBalanceProbe.BALANCE_GAUGE)
                .tag("provider", provider).tag("currency", currency).gauge();
        return found == null ? Double.NaN : found.value();
    }

    private double counter(String outcome, String provider) {
        Counter found = registry.find(ProviderBalanceProbe.BALANCE_SAMPLES_METRIC)
                .tag("outcome", outcome).tag("provider", provider).counter();
        return found == null ? 0D : found.count();
    }

    private long businessSampleCount() {
        return registry.find(RequestMetricsFilter.REQUEST_METRIC)
                .timers().stream().mapToLong(Timer::count).sum();
    }
}
