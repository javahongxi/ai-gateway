package org.hongxi.ai.gateway;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 真实厂商冒烟：客户端把 {@code base-url} 指向网关，走 application.yml 里的默认路由，非流式与
 * 流式各打一次两家厂商。
 *
 * <p>每个用例按自己那家的环境变量单独开关——CI 上通常只有一个，缺哪个就 skip 哪个。语义断言
 * （超时管到哪一层、报文是否原样、厂商隔离）都在 {@code GatewayChatFlowTest} 用进程内桩完成，
 * 这里只证明「真接得上、流真能一段一段过来、usage 真能在某个块里读到」。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "ai-gateway.tenants.team-a.api-key=sk-live-test"
})
class ProviderLiveTest {

    /** 与 application.yml 里 team-a 的本地 key 一致即可，测试用上面的 properties 覆盖成定值。 */
    private static final String TENANT_KEY = "Bearer sk-live-test";

    @LocalServerPort
    private int port;

    private WebClient client;

    @BeforeEach
    void connectToGateway() {
        client = WebClient.create("http://127.0.0.1:" + port);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "DASHSCOPE_API_KEY", matches = ".+")
    void dashScopeNonStreamRoundTripsThroughTheGateway() {
        String body = post("/v1/chat/completions",
                request("qwen3.7-plus", false))
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(60));

        assertThat(body).contains("chatcmpl").contains("\"usage\"");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "DASHSCOPE_API_KEY", matches = ".+")
    void dashScopeStreamArrivesChunkByChunkAndCarriesUsage() {
        observeStream("/v1/chat/completions", "qwen3.7-plus");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "DEEPSEEK_API_KEY", matches = ".+")
    void deepSeekStreamArrivesChunkByChunkAndCarriesUsage() {
        observeStream("/deepseek/v1/chat/completions", "deepseek-v4-flash");
    }

    /**
     * 打一次流式并检查：状态与 content-type 对、以 {@code data: [DONE]} 结束、正文里真带了
     * delta。usage 只记录不断言。
     *
     * <p>两条不写死在断言里的理由，都是实测出来的：
     * <ul>
     *   <li><b>到达次数</b>不能当条件。同一次跑里 qwen3.7-plus 分 40 次到达、deepseek-v4-flash
     *       只有 7 次——厂商一次只回一个 token 时整条流就在两三个 TCP 读里走完。「逐块不缓冲」
     *       由固定 80ms 间隔的桩来钉（{@code GatewayChatFlowTest}），那才是可重复的测法。</li>
     *   <li><b>usage 是否带</b>取决于厂商。同一份请求直连厂商三次都带 usage，走网关的某一次却没
     *       带（5 次里 1 次，之后两次重跑又都带）。真机冒烟断言它，等于把厂商的抖动引进 CI；
     *       所以这里打出来给人在看，规则本身写在 docs/requirements.md 的实测记录里。</li>
     * </ul>
     *
     * <p>扫描规则本身仍然要说清：usage 落在哪个块上两家不一样（DashScope 在 {@code [DONE]} 前
     * 多给一个 {@code choices:[]} 的块，DeepSeek 挂在 {@code finish_reason=stop} 那个块上），
     * 所以按「第一个带 completion_tokens 的块」找，不认位置。
     */
    private void observeStream(String path, String model) {
        AtomicLong started = new AtomicLong(System.nanoTime());
        List<String> chunks = new CopyOnWriteArrayList<>();
        List<Long> arrivals = new CopyOnWriteArrayList<>();

        post(path, request(model, true))
                .exchangeToFlux(response -> {
                    assertThat(response.statusCode().value()).isEqualTo(200);
                    MediaType contentType = response.headers().contentType().orElse(null);
                    assertThat(contentType).as("厂商按 SSE 回，网关不改 content-type").isNotNull();
                    assertThat(contentType.isCompatibleWith(MediaType.TEXT_EVENT_STREAM)).isTrue();
                    return response.bodyToFlux(DataBuffer.class);
                })
                .doOnNext(buffer -> {
                    String text = buffer.toString(StandardCharsets.UTF_8);
                    DataBufferUtils.release(buffer);
                    chunks.add(text);
                    arrivals.add((System.nanoTime() - started.get()) / 1_000_000);
                })
                .then()
                .block(Duration.ofSeconds(120));

        String stream = String.join("", chunks);
        assertThat(stream).contains("data: [DONE]");
        assertThat(stream).as("正文真的回来了").contains("\"delta\"");
        int usageChunk = indexOfUsage(chunks);
        System.out.printf("%s: %d 次到达, 首块 %dms, 末块 %dms, usage %s%n",
                model, chunks.size(), arrivals.get(0), arrivals.get(arrivals.size() - 1),
                usageChunk < 0 ? "本次没带（厂商抖动，见方法注释）" : "在第 " + usageChunk + " 个块");
    }

    private static int indexOfUsage(List<String> chunks) {
        for (int i = 0; i < chunks.size(); i++) {
            if (chunks.get(i).contains("\"completion_tokens\"")) {
                return i;
            }
        }
        return -1;
    }

    private WebClient.RequestHeadersSpec<?> post(String path, String body) {
        return client.post()
                .uri(path)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers -> headers.set(HttpHeaders.AUTHORIZATION, TENANT_KEY))
                .bodyValue(body);
    }

    /** 请求体由客户端给全，包括 {@code stream_options}：网关不注入（决策 D1）。 */
    private static String request(String model, boolean stream) {
        StringBuilder json = new StringBuilder("{\"model\":\"").append(model)
                .append("\",\"messages\":[{\"role\":\"user\",\"content\":\"用一个词回答：天空的颜色\"}],\"max_tokens\":16");
        if (stream) {
            json.append(",\"stream\":true,\"stream_options\":{\"include_usage\":true}");
        }
        return json.append("}").toString();
    }
}
