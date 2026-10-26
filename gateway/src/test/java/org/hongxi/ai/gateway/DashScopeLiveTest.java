package org.hongxi.ai.gateway;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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
 * 真实厂商冒烟：客户端把 {@code base-url} 指向网关，走 application.yml 里的默认路由打到
 * DashScope 的 OpenAI 兼容端点，非流式与流式各一次。
 *
 * <p>缺 {@code DASHSCOPE_API_KEY} 就 skip——CI 上没有密钥，也不该有。语义断言（超时管到哪一层、
 * 报文是否原样）都在 {@code GatewayChatFlowTest} 用进程内桩完成，这里只证明「真接得上、流真能一段
 * 一段过来」。
 */
@EnabledIfEnvironmentVariable(named = "DASHSCOPE_API_KEY", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.config.import=",
        "spring.cloud.nacos.config.import-check.enabled=false",
        "ai-gateway.tenants.team-a.api-key=sk-live-test"
})
class DashScopeLiveTest {

    private static final String CHAT_PATH = "/v1/chat/completions";

    private static final String MODEL = "qwen3.7-plus";

    @LocalServerPort
    private int port;

    private WebClient client;

    @BeforeEach
    void connectToGateway() {
        client = WebClient.create("http://127.0.0.1:" + port);
    }

    @Test
    void nonStreamCallRoundTripsThroughTheGateway() {
        String body = client.post()
                .uri(CHAT_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers -> headers.set(HttpHeaders.AUTHORIZATION, "Bearer sk-live-test"))
                .bodyValue(request(false))
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(60));

        assertThat(body).contains("chatcmpl").contains("\"usage\"");
    }

    @Test
    void streamArrivesChunkByChunkAndCarriesUsage() {
        AtomicLong started = new AtomicLong(System.nanoTime());
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        List<String> chunks = new CopyOnWriteArrayList<>();

        client.post()
                .uri(CHAT_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .headers(headers -> headers.set(HttpHeaders.AUTHORIZATION, "Bearer sk-live-test"))
                .bodyValue(request(true))
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
        assertThat(chunks).as("多次到达，不是一次性缓冲").hasSizeGreaterThan(3);
        assertThat(stream).contains("data: [DONE]");
        assertThat(arrivals.get(0))
                .as("首块远早于末块")
                .isLessThan(arrivals.get(arrivals.size() - 1));
        assertThat(stream)
                .as("include_usage 生效：某个块里带非空 usage，token 计量可以只读块、不改块")
                .containsPattern("\"usage\":\\{[^}]*completion_tokens");
        System.out.printf("live stream: %d 次到达, 首块 %dms, 末块 %dms%n",
                chunks.size(), arrivals.get(0), arrivals.get(arrivals.size() - 1));
    }

    /** 请求体由客户端给全，包括 {@code stream_options}：网关不注入（决策 D1）。 */
    private static String request(boolean stream) {
        StringBuilder json = new StringBuilder("{\"model\":\"").append(MODEL)
                .append("\",\"messages\":[{\"role\":\"user\",\"content\":\"用一个词回答：确定\"}],\"max_tokens\":16");
        if (stream) {
            json.append(",\"stream\":true,\"stream_options\":{\"include_usage\":true}");
        }
        return json.append("}").toString();
    }
}
