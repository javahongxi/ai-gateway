package org.hongxi.ai.gateway;

import java.time.Duration;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.hongxi.ai.gateway.support.AlwaysAllowRateLimiter;
import org.hongxi.ai.gateway.support.GatewayCalls;
import org.hongxi.ai.gateway.support.GatewayCalls.LiveCall;
import org.hongxi.ai.gateway.support.GatewayCalls.Outcome;
import org.hongxi.ai.gateway.support.StubUpstream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * R6 熔断与 failover：主厂商不可用时改道备用厂商，熔断打开之后不再去敲挂掉的门。
 *
 * <p>机制是 spring-cloud-gateway 的 {@code CircuitBreaker} 过滤器：配了 {@code fallbackUri} 之后，
 * 它在失败时把 {@code GATEWAY_REQUEST_URL_ATTR} 换成 fallback 路径、清掉响应状态、
 * {@code ServerWebExchangeUtils.reset(exchange)} 释放上游连接，再走一遍 DispatcherHandler——
 * 所以 fallback 能被<b>另一条路由</b>接住，这就是改道到备用厂商的通路。它同时会
 * {@code enableBodyCaching(routeId)}，请求体因此可重读（文档里那条未测的卡点由这段代码回答）。
 *
 * <p>两条实测出来的边界钉在这里：过滤器顺序（见 {@link #route} 的注释）与「响应已经提交之后
 * 改不了道」（{@link #midStreamFailureCannotBeFailedOver}）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "logging.level.com.alibaba.cloud.nacos.configdata.NacosConfigDataLoader=OFF",
        "ai-gateway.upstream.idle-timeout=0",
        "management.server.port=0",
        "ai-gateway.tenants.alpha.api-key=sk-alpha",
        "ai-gateway.tenants.alpha.providers[0]=dead",
        "ai-gateway.tenants.alpha.providers[1]=backup",
        "ai-gateway.tenants.alpha.providers[2]=flaky",
        // 计数窗口 4 次、失败率 50%：4 次全错必然打开
        "resilience4j.circuitbreaker.configs.default.sliding-window-type=count_based",
        "resilience4j.circuitbreaker.configs.default.sliding-window-size=2",
        "resilience4j.circuitbreaker.configs.default.minimum-number-of-calls=2",
        "resilience4j.circuitbreaker.configs.default.failure-rate-threshold=50",
        // 打开后别自动进半开：要看的是「不再敲主厂商的门」
        "resilience4j.circuitbreaker.configs.default.wait-duration-in-open-state=2s",
        "resilience4j.circuitbreaker.configs.default.permitted-number-of-calls-in-half-open-state=1",
        // 与生产同一件事：spring-cloud-circuitbreaker 把每次调用包进 TimeLimiter，
        // 默认 1 秒——大模型没有 1 秒回来的调用，等于全部判超时走 fallback。见下面那条用例
        "resilience4j.timelimiter.configs.default.timeout-duration=120s"
})
@Import(AlwaysAllowRateLimiter.class)
class CircuitBreakerFailoverTest {

    private static final String DEAD_PRIMARY_PATH = "/v1/chat/completions";

    private static final String FLAKY_PRIMARY_PATH = "/flaky/v1/chat/completions";

    private static final String BACKUP_PATH = "/backup/v1/chat/completions";

    private static final String REQUEST_BODY =
            "{\"model\":\"stub-model\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}";

    private static final StubUpstream upstream = StubUpstream.start();

    @LocalServerPort
    private int port;

    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    private GatewayCalls calls;

    @DynamicPropertySource
    static void gatewayConfiguration(DynamicPropertyRegistry registry) {
        // 主厂商 A：端口根本没人听，模拟「厂商整体不可用」。
        // 注意代价：每次连它要几秒才失败，所以这一维的用例数量要省着用。
        route(registry, 0, "dead-primary", "http://127.0.0.1:1", DEAD_PRIMARY_PATH, "dead", "dead-provider");
        // 备用厂商：两条主路由的 fallback 都指向它
        route(registry, 1, "backup", upstream.baseUrl(), BACKUP_PATH, "backup", null);
        // 主厂商 B：连得上，但会回 500 / 流到一半断，模拟「厂商活着、这次不行」
        route(registry, 2, "flaky-primary", upstream.baseUrl(), FLAKY_PRIMARY_PATH, "flaky", "flaky-provider");
    }

    private static void route(DynamicPropertyRegistry registry, int index, String id, String uri, String path,
            String provider, String breakerName) {
        String route = "spring.cloud.gateway.server.webflux.routes[" + index + "]";
        registry.add(route + ".id", () -> id);
        registry.add(route + ".uri", () -> uri);
        registry.add(route + ".predicates[0]", () -> "Path=" + path);
        registry.add(route + ".metadata.provider", () -> provider);
        // 连不上时最多等 200ms。不设的话 SCG 用全局默认，连一个没人听的端口要耗掉约 10 秒，
        // 这条类里 9 次这样的连接就是 90 秒——不是被测行为，是环境噪声。
        registry.add(route + ".metadata[connect-timeout]", () -> "200");
        if (breakerName == null) {
            // 备用路由自己不再挂熔断：它就是最后那道防线
            registry.add(route + ".filters[0]", () -> "SetPath=/upstream/" + provider);
            registry.add(route + ".filters[1]", () -> "SetRequestHeader=Authorization,Bearer " + provider + "-secret");
            return;
        }
        // 顺序有讲究：CircuitBreaker 要排在 SetPath / SetRequestHeader 之前。它改道时用的是
        // 「自己看到的那个 request」——若 SetRequestHeader 已在前面把厂商 key 换进去，重投递时
        // 鉴权过滤器读到的是厂商 key，直接 401（实测踩过）。
        registry.add(route + ".filters[0].name", () -> "CircuitBreaker");
        registry.add(route + ".filters[0].args.name", () -> breakerName);
        registry.add(route + ".filters[0].args.fallbackUri", () -> "forward:" + BACKUP_PATH);
        registry.add(route + ".filters[0].args.statusCodes[0]", () -> "INTERNAL_SERVER_ERROR");
        registry.add(route + ".filters[1]", () -> "SetPath=/upstream/" + provider);
        registry.add(route + ".filters[2]", () -> "SetRequestHeader=Authorization,Bearer " + provider + "-secret");
    }

    @BeforeEach
    void connectAndResetBreakers() {
        calls = new GatewayCalls("http://127.0.0.1:" + port, REQUEST_BODY);
        upstream.reset();
        upstream.scenario(StubUpstream.Scenario.ECHO_JSON);
        allBreakers().forEach(io.github.resilience4j.circuitbreaker.CircuitBreaker::transitionToClosedState);
    }

    @Test
    void deadPrimaryIsServedByTheBackup() {
        Outcome outcome = calls.call(DEAD_PRIMARY_PATH, "Bearer sk-alpha");

        assertThat(outcome.status())
                .as("主厂商连不上，改道之后客户端拿到的是备用的 200")
                .isEqualTo(HttpStatus.OK.value());
        assertThat(outcome.body()).contains("chatcmpl-stub");
        assertThat(upstream.hits("/upstream/backup")).isEqualTo(1);
        assertThat(upstream.authorization()).as("备用厂商拿到的是它自己的 key").isEqualTo("Bearer backup-secret");
        assertThat(upstream.tenant()).as("租户身份在改道之后仍然带着").isEqualTo("alpha");
    }

    @Test
    void breakerOpensAfterAWindowOfFailures() {
        for (int i = 0; i < 2; i++) {
            assertThat(calls.call(DEAD_PRIMARY_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.OK.value());
        }

        assertThat(breaker("dead-provider"))
                .as("4 次全失败，窗口 4、阈值 50%：该打开了")
                .get()
                .satisfies(b -> assertThat(b.getState()).isEqualTo(CircuitBreaker.State.OPEN));
    }

    @Test
    void openBreakerStillServesTheBackupWithoutWaitingOnThePrimary() {
        // 2 次把熔断打开（每次都要真连一次死端口，所以这一类用例只留这一条吃这个成本）
        for (int i = 0; i < 2; i++) {
            calls.call(DEAD_PRIMARY_PATH, "Bearer sk-alpha");
        }
        int before = upstream.requests();

        long started = System.nanoTime();
        assertThat(calls.call(DEAD_PRIMARY_PATH, "Bearer sk-alpha").status()).isEqualTo(HttpStatus.OK.value());

        assertThat(upstream.requests() - before)
                .as("熔断打开后主厂商不再被敲，只有备用的那一次")
                .isEqualTo(1);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .as("CallNotPermitted 立刻走 fallback，不必再连一次死端口")
                .isLessThan(Duration.ofSeconds(2));
    }

    @Test
    void serverErrorFromALivePrimaryIsNotReplacedByTheBackup() {
        upstream.scenario(StubUpstream.Scenario.FAIL_500);

        Outcome outcome = calls.call(FLAKY_PRIMARY_PATH, "Bearer sk-alpha");

        // 实测：熔断器认这个 500（备用确实被打了），但客户端拿到的还是主厂商那个 500——
        // 响应已经写出去了，fallback 改不了它。要让 5xx 也能改道，得先把响应扣住不写，
        // 那是另一套机制，见 docs/requirements.md 决策 D6。
        assertThat(outcome.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(outcome.body()).contains("upstream exploded");
        assertThat(upstream.hits("/upstream/backup")).as("熔断确实介入了，只是改不了已写出的响应").isGreaterThan(0);
    }

    @Test
    void midStreamFailureCannotBeFailedOver() {
        upstream.scenario(StubUpstream.Scenario.SSE_THEN_BREAK);

        LiveCall live = calls.start(FLAKY_PRIMARY_PATH, "Bearer sk-alpha");
        live.awaitQuietly(Duration.ofSeconds(3));

        assertThat(live.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(live.body()).as("第一块已经交付给客户端").contains("Hi");
        assertThat(live.terminated()).as("流以失败收尾").isTrue();
        assertThat(upstream.hits("/upstream/backup"))
                .as("响应头随第一块提交之后就没有改道的余地了")
                .isZero();
    }

    /** registry 的 API 返回 vavr 的 Option，这里换成 Optional，断言读起来正常。 */
    private java.util.Optional<CircuitBreaker> breaker(String name) {
        return allBreakers().filter(b -> name.equals(b.getName())).findFirst();
    }

    private java.util.stream.Stream<CircuitBreaker> allBreakers() {
        return java.util.stream.StreamSupport.stream(circuitBreakers.getAllCircuitBreakers().spliterator(), false);
    }

    @Test
    void slowPrimaryIsNotMistakenForADeadOne() {
        upstream.scenario(StubUpstream.Scenario.SLOW_HEAD);
        // 1.5 秒才回响应头：比 TimeLimiter 的默认 1 秒慢。把上面那条
        // resilience4j.timelimiter...timeout-duration 去掉，这条就会红——客户端拿到的是
        // 备用的响应，而主厂商其实活着。真机上撞过一次：qwen 请求被改道到 DeepSeek，
        // 拿回来的是「supported model names are deepseek-flash…」的 400。

        Outcome outcome = calls.call(FLAKY_PRIMARY_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.OK.value());
        assertThat(outcome.body()).contains("chatcmpl-slow");
        assertThat(upstream.hits("/upstream/backup"))
                .as("慢不等于死：不该改道")
                .isZero();
    }

    @Test
    void connectionResetAfterTheRequestWasSentStillReturnsThePrimaryFailure() {
        // 与「连不上」相对的那一半：连上了、请求写出去了、然后被掐断。熔断器认这个失败并
        // 打了备用厂商，但客户端拿到的还是 500——响应已经开始往客户端走了，改道结果替换不了它。
        // 与 5xx 那条是同一类边界（见 serverErrorFromALivePrimaryIsNotReplacedByTheBackup）。
        upstream.scenario(StubUpstream.Scenario.RESET);

        Outcome outcome = calls.call(FLAKY_PRIMARY_PATH, "Bearer sk-alpha");

        assertThat(outcome.status()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(upstream.hits("/upstream/backup"))
                .as("熔断确实改道了，只是客户端看不到")
                .isEqualTo(1);
    }

    @Test
    void eachPrimaryRouteHasItsOwnBreaker() {
        calls.call(DEAD_PRIMARY_PATH, "Bearer sk-alpha");
        calls.call(FLAKY_PRIMARY_PATH, "Bearer sk-alpha");

        assertThat(breaker("dead-provider")).isPresent();
        assertThat(breaker("flaky-provider"))
                .as("一家挂了不该把另一家也短路")
                .isPresent();
    }
}
